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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.getRestClusterClientSupplier;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/** End-to-end test for {@link DelayedScaleDown}. */
public class DelayedScaleDownEndToEndTest {

    private final int INITIAL_SOURCE_PARALLELISM = 200;
    private final int INITIAL_SINK_PARALLELISM = 1000;

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

        var scaleDownInterval = Duration.ofMinutes(60);
        // The metric window size is 9:59 to avoid other metrics are mixed.
        var metricWindow = Duration.ofMinutes(9).plus(Duration.ofSeconds(59));

        var defaultConf = context.getConfiguration();
        defaultConf.set(AutoScalerOptions.AUTOSCALER_ENABLED, true);
        defaultConf.set(AutoScalerOptions.SCALING_ENABLED, true);
        defaultConf.set(AutoScalerOptions.STABILIZATION_INTERVAL, Duration.ZERO);
        defaultConf.set(AutoScalerOptions.RESTART_TIME, Duration.ofSeconds(0));
        defaultConf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ofSeconds(0));
        defaultConf.set(AutoScalerOptions.SCALING_ENABLED, true);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_DOWN_FACTOR, 1.);
        defaultConf.set(AutoScalerOptions.MAX_SCALE_UP_FACTOR, (double) Integer.MAX_VALUE);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION, 0.8);
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.2);
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
        // The sink busy time list for each window.
        var sinkBusyList = List.of(100, 300, 150, 200, 400, 250, 100);

        var totalRecords = 0L;
        int recordsPerMinutes = 4800000;

        for (int windowIndex = 0; windowIndex <= 6; windowIndex++) {
            for (int i = 1; i <= 10; i++) {
                now = now.plus(Duration.ofMinutes(1));
                setClocksTo(now);

                metricsCollector.updateMetrics(source, buildMetric(totalRecords, 800));
                metricsCollector.updateMetrics(
                        sink, buildMetric(totalRecords, sinkBusyList.get(windowIndex)));

                autoscaler.scale(context);
                // Metric window is 10 minutes, so 10 is the maximal metric size.
                expectedMetricSize = Math.min(expectedMetricSize + 1, 10);
                assertCollectedMetricsSize(expectedMetricSize);

                // Assert the recommended parallelism.
                if (windowIndex == 6 && i == 10) {
                    // The utilization target is 0.8, and busyTimePerSec is 800 ms, so source
                    // parallelism won't be changed.
                    assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM))
                            .isEqualTo(INITIAL_SOURCE_PARALLELISM);

                    // Last metric, we expect scale down is executed for sink, and the max
                    // recommended parallelism in the past scale down interval window should be
                    // used.
                    // The max busy time needs more parallelism than others, so we could compute
                    // parallelism based on the max busy time.
                    var maxBusyTime = sinkBusyList.stream().max(Comparator.naturalOrder()).get();
                    var sinkMaxBusyRatio = 1.0d * maxBusyTime / 1000;
                    var expectedSinkParallelism =
                            (int) (INITIAL_SINK_PARALLELISM * sinkMaxBusyRatio / 0.8);
                    assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM))
                            .isEqualTo(expectedSinkParallelism);

                    // Check scaling realizer.
                    assertThat(scalingRealizer.events).hasSize(1);
                    var parallelismOverrides =
                            scalingRealizer.events.poll().getParallelismOverrides();
                    assertThat(parallelismOverrides)
                            .containsEntry(
                                    source.toHexString(),
                                    Integer.toString(INITIAL_SOURCE_PARALLELISM));
                    assertThat(parallelismOverrides)
                            .containsEntry(
                                    sink.toHexString(), Integer.toString(expectedSinkParallelism));
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

    /**
     * Initially, all tasks are scaled down within the utilization bound, and scaling down only
     * occurs when any task is outside the utilization bound.
     */
    @Test
    void testScaleDownWithInUtilizationBoundary() throws Exception {
        // The busy time list for each window.
        // The recommended parallelism is 300 for the ninth metric window, but it doesn't take
        // effect since it's not the max recommended parallelism in the past scale down interval
        // window.
        var sourceBusyList = List.of(800, 700, 720, 750, 730, 720, 700, 710, 300, 700);
        var sinkBusyList = List.of(800, 800, 800, 800, 800, 800, 800);

        var totalRecords = 0L;
        int recordsPerMinutes = 4800000;

        for (int windowIndex = 0; windowIndex <= 6; windowIndex++) {
            for (int i = 1; i <= 10; i++) {
                now = now.plus(Duration.ofMinutes(1));
                setClocksTo(now);

                metricsCollector.updateMetrics(source, buildMetric(totalRecords, 800));
                metricsCollector.updateMetrics(
                        sink, buildMetric(totalRecords, sinkBusyList.get(windowIndex)));

                autoscaler.scale(context);
                // Metric window is 10 minutes, so 10 is the maximal metric size.
                expectedMetricSize = Math.min(expectedMetricSize + 1, 10);
                assertCollectedMetricsSize(expectedMetricSize);

                // Assert the recommended parallelism.
                if (windowIndex == 6 && i == 10) {
                    // The utilization target is 0.8, and busyTimePerSec is 800 ms, so source
                    // parallelism won't be changed.
                    assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM))
                            .isEqualTo(INITIAL_SOURCE_PARALLELISM);

                    // Last metric, we expect scale down is executed for sink, and max recommended
                    // parallelism in the past window should be used.
                    // The max busy time needs more parallelism than others, so we could compute
                    // parallelism based on the max busy time.
                    var maxBusyTime = sinkBusyList.stream().max(Comparator.naturalOrder()).get();
                    var sinkMaxBusyRatio = 1.0d * maxBusyTime / 1000;
                    var expectedSinkParallelism =
                            (int) (INITIAL_SINK_PARALLELISM * sinkMaxBusyRatio / 0.8);
                    assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM))
                            .isEqualTo(expectedSinkParallelism);

                    // Check scaling realizer.
                    assertThat(scalingRealizer.events).hasSize(1);
                    var parallelismOverrides =
                            scalingRealizer.events.poll().getParallelismOverrides();
                    assertThat(parallelismOverrides)
                            .containsEntry(
                                    source.toHexString(),
                                    Integer.toString(INITIAL_SOURCE_PARALLELISM));
                    assertThat(parallelismOverrides)
                            .containsEntry(
                                    sink.toHexString(), Integer.toString(expectedSinkParallelism));
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

    // todo :
    // 1. [done] The scale down won't be executed before scale down interval window is full
    // 2. The trigger time will be cleaned up, when other tasks scale up
    // 3. The trigger time will be cleaned up, when other tasks scale down
    // 4. [done] It will use max recommended parallelism in the past window size when scale down is
    // executed.
    // 5. [doing] All tasks are scaled down within utilization boundary, and scale down could happen
    // after outside of the boundary.
    // 6. The triggered scale down will be canceled when parallelism is greater than or equal to the
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
