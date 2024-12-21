/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.event.TestingEventCollector;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.metrics.TestMetrics;
import org.apache.flink.autoscaler.realizer.TestingScalingRealizer;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.getRestClusterClientSupplier;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/** End-to-end test for {@link DelayedScaleDown}. */
public class DelayedScaleDownEndToEndTest {

    private static final int INITIAL_SOURCE_PARALLELISM = 200;
    private static final int INITIAL_SINK_PARALLELISM = 1000;
    private static final double UTILIZATION_TARGET = 0.8;

    private JobAutoScalerContext<JobID> context;
    private AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer;

    private TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>> metricsCollector;

    private JobVertexID source, sink;

    private JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;
    private Instant now;
    private int expectedMetricSize;

    @BeforeEach
    public void setup() throws Exception {
        context = createDefaultJobAutoScalerContext();

        TestingEventCollector<JobID, JobAutoScalerContext<JobID>> eventCollector =
                new TestingEventCollector<>();
        stateStore = new InMemoryAutoScalerStateStore<>();

        source = new JobVertexID();
        sink = new JobVertexID();

        metricsCollector =
                new TestingMetricsCollector<>(
                        new JobTopology(
                                new VertexInfo(source, Map.of(), INITIAL_SOURCE_PARALLELISM, 4000),
                                new VertexInfo(
                                        sink,
                                        Map.of(source, REBALANCE),
                                        INITIAL_SINK_PARALLELISM,
                                        4000)));

        var scaleDownInterval = Duration.ofMinutes(60).minus(Duration.ofSeconds(1));
        // The metric window size is 9:59 to avoid other metrics are mixed.
        var metricWindow = Duration.ofMinutes(10).minus(Duration.ofSeconds(1));

        var defaultConf = context.getConfiguration();
        defaultConf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        defaultConf.set(AutoScalerOptions.SCALING_ENABLED, true);
        defaultConf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        defaultConf.set(AutoScalerOptions.RESTART_TIME, Duration.ofSeconds(0));
        defaultConf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ofSeconds(0));
        defaultConf.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 10000);
        defaultConf.set(AutoScalerOptions.SCALING_ENABLED, true);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION, UTILIZATION_TARGET);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.1);
        defaultConf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, scaleDownInterval);
        defaultConf.set(AutoScalerOptions.METRICS_WINDOW, metricWindow);

        scalingRealizer = new TestingScalingRealizer<>();
        autoscaler =
                new JobAutoScalerImpl<>(
                        metricsCollector,
                        new ScalingMetricEvaluator(),
                        new ScalingExecutor<>(eventCollector, stateStore),
                        eventCollector,
                        scalingRealizer,
                        stateStore);

        // initially the last evaluated metrics are empty
        assertNull(autoscaler.lastEvaluatedMetrics.get(context.getJobKey()));

        now = Instant.ofEpochMilli(0);
        setClocksTo(now);
        running(now);

        metricsCollector.updateMetrics(source, buildMetric(0, 800));
        metricsCollector.updateMetrics(sink, buildMetric(0, 800));

        // the recommended parallelism values are empty initially
        autoscaler.scale(context);
        expectedMetricSize = 1;
        assertCollectedMetricsSize(expectedMetricSize);
    }

    /**
     * The scale down won't be executed before scale down interval window is full, and it will use
     * the max recommended parallelism in the past scale down interval window size when scale down
     * is executed.
     */
    @Test
    void testDelayedScaleDownHappen() throws Exception {
        var sourceBusyList = List.of(800, 800, 800, 800, 800, 800, 800);
        var sinkBusyList = List.of(350, 300, 150, 200, 400, 250, 100);

        scalingHappensInLastMetricWindow(sourceBusyList, sinkBusyList);
    }

    /**
     * Initially, all tasks are scaled down within the utilization bound, and scaling down only
     * occurs when any task is outside the utilization bound.
     */
    @Test
    void testScaleDownWithInUtilizationBoundary() throws Exception {
        // The busy time list for each window.
        // 由于最终的并行度是 scale down interval window 的最大值，所以只有当 scale down interval 窗口内所有的 busy time 都小于
        //  1000 * (utilization target - utilization bound) 时，scale down 才会被执行。
        var sourceBusyList =
                List.of(
                        800, 700, 620, 750, 650, 720, 700, 710, 300, 610, 590, 400, 580, 560, 630,
                        520, 570, 500, 400, 530, 480);
        var sinkBusyList =
                List.of(
                        800, 800, 800, 800, 800, 800, 800, 780, 720, 620, 750, 650, 720, 700, 680,
                        660, 580, 620, 640, 600, 590);

        scalingHappensInLastMetricWindow(sourceBusyList, sinkBusyList);
    }

    // Scale down will be executed in the last metric window.
    private void scalingHappensInLastMetricWindow(
            List<Integer> sourceBusyList, List<Integer> sinkBusyList) throws Exception {
        assertThat(sourceBusyList).hasSameSizeAs(sinkBusyList);
        var metricWindowSize = sourceBusyList.size();

        assertThat(metricWindowSize).isGreaterThan(6);

        var totalRecords = 0L;
        int recordsPerMinutes = 4800000;

        for (int windowIndex = 0; windowIndex < metricWindowSize; windowIndex++) {
            for (int i = 1; i <= 10; i++) {
                now = now.plus(Duration.ofMinutes(1));
                setClocksTo(now);

                metricsCollector.updateMetrics(
                        source, buildMetric(totalRecords, sourceBusyList.get(windowIndex)));
                metricsCollector.updateMetrics(
                        sink, buildMetric(totalRecords, sinkBusyList.get(windowIndex)));

                autoscaler.scale(context);
                // Metric window is 10 minutes, so 10 is the maximal metric size.
                expectedMetricSize = Math.min(expectedMetricSize + 1, 10);
                assertCollectedMetricsSize(expectedMetricSize);

                // Assert the recommended parallelism.
                if (windowIndex == metricWindowSize - 1 && i == 10) {
                    // Last metric, we expect scale down is executed, and max recommended
                    // parallelism in the past window should be used.
                    // The max busy time needs more parallelism than others, so we could compute
                    // parallelism based on the max busy time.
                    var expectedSourceParallelism =
                            getExpectedParallelism(sourceBusyList, INITIAL_SOURCE_PARALLELISM);
                    var expectedSinkParallelism =
                            getExpectedParallelism(sinkBusyList, INITIAL_SINK_PARALLELISM);
                    pollAndAssertScalingRealizer(
                            expectedSourceParallelism, expectedSinkParallelism);
                } else {
                    // Otherwise, scale down cannot be executed.
                    if (windowIndex == 0 && i <= 9) {
                        // Metric window is not full, so don't have recommended parallelism.
                        assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM)).isNull();
                        assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM)).isNull();
                    } else {
                        // Scale down won't be executed before scale down interval window is full.
                        assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM))
                                .isEqualTo(INITIAL_SOURCE_PARALLELISM);
                        assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM))
                                .isEqualTo(INITIAL_SINK_PARALLELISM);
                    }
                    assertThat(scalingRealizer.events).isEmpty();
                }

                totalRecords += recordsPerMinutes;
            }
        }
    }

    /** The scale down trigger time will be reset, when other tasks scale up. */
    @Test
    void testDelayedScaleDownIsResetWhenAnotherTaskScaleUp() throws Exception {
        // It expects delayed scale down of sink is reset when scale up is executed for source.
        var sourceBusyList = List.of(800, 800, 800, 800, 950);
        var sinkBusyList = List.of(200, 200, 200, 200, 200);

        var sourceBusyWindow = new LinkedList<Integer>();

        var metricWindowSize = sourceBusyList.size();

        var totalRecords = 0L;
        int recordsPerMinutes = 4800000;

        for (int windowIndex = 0; windowIndex < metricWindowSize; windowIndex++) {
            for (int i = 1; i <= 10; i++) {
                now = now.plus(Duration.ofMinutes(1));
                setClocksTo(now);

                var busyTimePerSec = sourceBusyList.get(windowIndex);
                sourceBusyWindow.add(busyTimePerSec);
                // Poll the oldest metric.
                if (sourceBusyWindow.size() > 10) {
                    sourceBusyWindow.pollFirst();
                }

                metricsCollector.updateMetrics(source, buildMetric(totalRecords, busyTimePerSec));
                metricsCollector.updateMetrics(
                        sink, buildMetric(totalRecords, sinkBusyList.get(windowIndex)));

                autoscaler.scale(context);
                // Metric window is 10 minutes, so 10 is the maximal metric size.
                expectedMetricSize = Math.min(expectedMetricSize + 1, 10);
                assertCollectedMetricsSize(expectedMetricSize);

                // Assert the recommended parallelism.
                if (windowIndex == 0 && i <= 9) {
                    // Metric window is not full, so don't have recommended parallelism.
                    assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM)).isNull();
                    assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM)).isNull();
                    assertThat(scalingRealizer.events).isEmpty();
                } else {
                    double busyAvg =
                            sourceBusyWindow.stream().mapToInt(e -> e).average().getAsDouble();
                    if (busyAvg > 900) {
                        // Scaling up happens for source, and the scale down of sink cannot be
                        // executed
                        // since the scale down interval window is not full.
                        var sourceMaxBusyRatio = busyAvg / 1000;
                        var expectedSourceParallelism =
                                (int)
                                        Math.ceil(
                                                INITIAL_SOURCE_PARALLELISM
                                                        * sourceMaxBusyRatio
                                                        / UTILIZATION_TARGET);
                        pollAndAssertScalingRealizer(
                                expectedSourceParallelism, INITIAL_SINK_PARALLELISM);
                        // The delayed scale down should be cleaned up after source scales up.
                        assertThat(stateStore.getDelayedScaleDown(context).getDelayedVertices())
                                .isEmpty();
                        break;
                    } else {
                        // Scale down won't be executed before source utilization within the
                        // utilization bound.
                        assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM))
                                .isEqualTo(INITIAL_SOURCE_PARALLELISM);
                        assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM))
                                .isEqualTo(INITIAL_SINK_PARALLELISM);
                        assertThat(scalingRealizer.events).isEmpty();
                        // Delayed scale down is triggered
                        assertThat(stateStore.getDelayedScaleDown(context).getDelayedVertices())
                                .isNotEmpty();
                    }
                }
                totalRecords += recordsPerMinutes;
            }
        }
    }

    /** The scale down trigger time will be reset, when other tasks scale down. */
    @Test
    void testDelayedScaleDownIsResetWhenAnotherTaskScaleDown() throws Exception {
        // It expects delayed scale down of sink is reset when scale down is executed for source.
        var sourceBusyList = List.of(200, 200, 200, 200, 200, 200, 200);
        var sinkBusyList = List.of(800, 800, 200, 200, 200, 200, 200);

        var metricWindowSize = sourceBusyList.size();

        var totalRecords = 0L;
        int recordsPerMinutes = 4800000;

        for (int windowIndex = 0; windowIndex < metricWindowSize; windowIndex++) {
            for (int i = 1; i <= 10; i++) {
                now = now.plus(Duration.ofMinutes(1));
                setClocksTo(now);

                metricsCollector.updateMetrics(
                        source, buildMetric(totalRecords, sourceBusyList.get(windowIndex)));
                metricsCollector.updateMetrics(
                        sink, buildMetric(totalRecords, sinkBusyList.get(windowIndex)));

                autoscaler.scale(context);
                // Metric window is 10 minutes, so 10 is the maximal metric size.
                expectedMetricSize = Math.min(expectedMetricSize + 1, 10);
                assertCollectedMetricsSize(expectedMetricSize);

                // Assert the recommended parallelism.
                if (windowIndex == 0 && i <= 9) {
                    // Metric window is not full, so don't have recommended parallelism.
                    assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM)).isNull();
                    assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM)).isNull();
                    assertThat(scalingRealizer.events).isEmpty();
                } else {
                    if (windowIndex == metricWindowSize - 1 && i == 10) {
                        // Scaling up happens for source, and the scale down of sink cannot be
                        // executed
                        // since the scale down interval window is not full.
                        var expectedSourceParallelism =
                                getExpectedParallelism(sourceBusyList, INITIAL_SOURCE_PARALLELISM);
                        pollAndAssertScalingRealizer(
                                expectedSourceParallelism, INITIAL_SINK_PARALLELISM);
                        // The delayed scale down should be cleaned up after source scales down.
                        assertThat(stateStore.getDelayedScaleDown(context).getDelayedVertices())
                                .isEmpty();
                    } else {
                        // Scale down won't be executed when scale down interval window of source is
                        // not full.
                        assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM))
                                .isEqualTo(INITIAL_SOURCE_PARALLELISM);
                        assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM))
                                .isEqualTo(INITIAL_SINK_PARALLELISM);
                        assertThat(scalingRealizer.events).isEmpty();
                        // Delayed scale down is triggered
                        assertThat(stateStore.getDelayedScaleDown(context).getDelayedVertices())
                                .isNotEmpty();
                    }
                }
                totalRecords += recordsPerMinutes;
            }
        }
    }

    private static List<List<Integer>> sinkParallelismIsGreaterOrEqualProvider() {
        return List.of(
                // test for the recommended parallelism is equal to the current parallelism.
                List.of(200, 200, 200, 200, 800),
                List.of(700, 700, 700, 700, 800),
                List.of(780, 780, 780, 780, 800),
                List.of(790, 800),
                List.of(760, 800),
                List.of(750, 800),
                List.of(350, 800),
                // test for the recommended parallelism is greater than current parallelism.
                List.of(200, 200, 200, 200, 850),
                List.of(700, 700, 700, 700, 850),
                List.of(780, 780, 780, 780, 850),
                List.of(790, 850),
                List.of(760, 850),
                List.of(750, 850),
                List.of(350, 850),
                List.of(200, 200, 200, 200, 900),
                List.of(700, 700, 700, 700, 900),
                List.of(780, 780, 780, 780, 900),
                List.of(790, 900),
                List.of(760, 900),
                List.of(750, 900),
                List.of(350, 900));
    }

    /**
     * The triggered scale down of sink will be canceled when the recommended parallelism is greater
     * than or equal to the current parallelism.
     */
    @ParameterizedTest
    @MethodSource("sinkParallelismIsGreaterOrEqualProvider")
    void testDelayedScaleDownIsCanceledWhenRecommendedParallelismIsGreaterOrEqual(
            List<Integer> sinkBusyList) throws Exception {
        var metricWindowSize = sinkBusyList.size();
        var sourceBusyList = Collections.nCopies(metricWindowSize, 800);

        var sinkBusyWindow = new LinkedList<Integer>();

        var totalRecords = 0L;
        int recordsPerMinutes = 480000000;

        for (int windowIndex = 0; windowIndex < metricWindowSize; windowIndex++) {
            for (int i = 1; i <= 10; i++) {
                now = now.plus(Duration.ofMinutes(1));
                setClocksTo(now);

                var busyTimePerSec = sinkBusyList.get(windowIndex);
                sinkBusyWindow.add(busyTimePerSec);
                // Poll the oldest metric.
                if (sinkBusyWindow.size() > 10) {
                    sinkBusyWindow.pollFirst();
                }

                metricsCollector.updateMetrics(
                        source, buildMetric(totalRecords, sourceBusyList.get(windowIndex)));
                metricsCollector.updateMetrics(sink, buildMetric(totalRecords, busyTimePerSec));

                autoscaler.scale(context);
                // Metric window is 10 minutes, so 10 is the maximal metric size.
                expectedMetricSize = Math.min(expectedMetricSize + 1, 10);
                assertCollectedMetricsSize(expectedMetricSize);

                assertThat(scalingRealizer.events).isEmpty();

                // Assert the recommended parallelism.
                if (windowIndex == 0 && i <= 9) {
                    // Metric window is not full, so don't have recommended parallelism.
                    assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM)).isNull();
                    assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM)).isNull();
                } else {
                    // Scale down won't be executed before scale down interval window is full.
                    assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM))
                            .isEqualTo(INITIAL_SOURCE_PARALLELISM);
                    assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM))
                            .isEqualTo(INITIAL_SINK_PARALLELISM);
                    assertThat(scalingRealizer.events).isEmpty();

                    double busyAvg =
                            sinkBusyWindow.stream().mapToInt(e -> e).average().getAsDouble();
                    if (busyAvg >= 800) {
                        // The delayed scale down should be cleaned up after the expected
                        // recommended parallelism is greater than or equal to the current
                        // parallelism.
                        assertThat(stateStore.getDelayedScaleDown(context).getDelayedVertices())
                                .isEmpty();
                        break;
                    } else {
                        // Delayed scale down is triggered
                        assertThat(stateStore.getDelayedScaleDown(context).getDelayedVertices())
                                .isNotEmpty();
                    }
                }
                totalRecords += recordsPerMinutes;
            }
        }
    }

    private static int getExpectedParallelism(List<Integer> taskBusyList, int currentParallelism) {
        var maxBusyTime =
                taskBusyList.stream()
                        .skip(taskBusyList.size() - 6)
                        .max(Comparator.naturalOrder())
                        .get();
        var maxBusyRatio = 1.0d * maxBusyTime / 1000;
        return (int) (currentParallelism * maxBusyRatio / UTILIZATION_TARGET);
    }

    private void pollAndAssertScalingRealizer(
            int expectedSourceParallelism, int expectedSinkParallelism) {
        // Assert metric
        assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM))
                .isEqualTo(expectedSourceParallelism);
        assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM))
                .isEqualTo(expectedSinkParallelism);

        // Check scaling realizer.
        assertThat(scalingRealizer.events).hasSize(1);
        var parallelismOverrides = scalingRealizer.events.poll().getParallelismOverrides();
        assertThat(parallelismOverrides)
                .containsEntry(source.toHexString(), Integer.toString(expectedSourceParallelism));
        assertThat(parallelismOverrides)
                .containsEntry(sink.toHexString(), Integer.toString(expectedSinkParallelism));
    }

    // todo :
    // 1. [done] The scale down won't be executed before scale down interval window is full
    // 2. [done] The trigger time will be cleaned up, when other tasks scale up
    // 3. [done] The trigger time will be cleaned up, when other tasks scale down
    // 4. [done] It will use max recommended parallelism in the past window size when scale down is
    // executed.
    // 5. [doing: testScaleDownWithInUtilizationBoundary] All tasks are scaled down within
    // utilization boundary, and scale down could happen
    // after outside of the boundary.
    // 6. [done ]The triggered scale down will be canceled when parallelism is greater than or equal
    // to the
    // current p.

    private void assertCollectedMetricsSize(int expectedSize) throws Exception {
        assertThat(stateStore.getCollectedMetrics(context)).hasSize(expectedSize);
    }

    private Double getCurrentMetricValue(JobVertexID jobVertexID, ScalingMetric scalingMetric) {
        var metric =
                autoscaler
                        .lastEvaluatedMetrics
                        .get(context.getJobKey())
                        .getVertexMetrics()
                        .get(jobVertexID)
                        .get(scalingMetric);
        return metric == null ? null : metric.getCurrent();
    }

    private void restart(Instant now) {
        metricsCollector.setJobUpdateTs(now);
        context =
                new JobAutoScalerContext<>(
                        context.getJobKey(),
                        context.getJobID(),
                        JobStatus.CREATED,
                        context.getConfiguration(),
                        context.getMetricGroup(),
                        getRestClusterClientSupplier());
    }

    private void running(Instant now) {
        metricsCollector.setJobUpdateTs(now);
        context =
                new JobAutoScalerContext<>(
                        context.getJobKey(),
                        context.getJobID(),
                        JobStatus.RUNNING,
                        context.getConfiguration(),
                        context.getMetricGroup(),
                        getRestClusterClientSupplier());
    }

    private void setClocksTo(Instant time) {
        var clock = Clock.fixed(time, ZoneId.systemDefault());
        metricsCollector.setClock(clock);
        autoscaler.setClock(clock);
    }

    private TestMetrics buildMetric(long totalRecords, int busyTimePerSec) {
        return TestMetrics.builder()
                .numRecordsIn(totalRecords)
                .numRecordsOut(totalRecords)
                .maxBusyTimePerSec(busyTimePerSec)
                .build();
    }
}
