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
import java.util.Map;

import static org.apache.flink.autoscaler.TestingAutoscalerUtils.createDefaultJobAutoScalerContext;
import static org.apache.flink.autoscaler.TestingAutoscalerUtils.getRestClusterClientSupplier;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;
import static org.apache.flink.autoscaler.topology.ShipStrategy.REBALANCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/** End-to-end test for {@link DelayedScaleDown}. */
public class DelayedScaleDownEndToEndTest {

    private JobAutoScalerContext<JobID> context;
    private AutoScalerStateStore<JobID, JobAutoScalerContext<JobID>> stateStore;

    private TestingMetricsCollector<JobID, JobAutoScalerContext<JobID>> metricsCollector;

    private TestingScalingRealizer<JobID, JobAutoScalerContext<JobID>> scalingRealizer;

    private JobVertexID source, sink;

    private JobAutoScalerImpl<JobID, JobAutoScalerContext<JobID>> autoscaler;
    private Instant now;

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
                                new VertexInfo(source, Map.of(), 100, 720),
                                new VertexInfo(sink, Map.of(source, REBALANCE), 200, 720)));

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
        defaultConf.set(AutoScalerOptions.TARGET_UTILIZATION_BOUNDARY, 0.1);

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

        metricsCollector.updateMetrics(
                source,
                TestMetrics.builder()
                        .numRecordsIn(0)
                        .numRecordsOut(0)
                        .maxBusyTimePerSec(800)
                        .build());
        metricsCollector.updateMetrics(
                sink, TestMetrics.builder().numRecordsIn(0).maxBusyTimePerSec(100).build());

        // the recommended parallelism values are empty initially
        autoscaler.scale(context);
        assertCollectedMetricsSize(1);
    }

    /**
     * The scale down won't be executed before scale down interval window is full, and it will use
     * max parallelism in the past window size when scale down is executed.
     */
    @Test
    void testDelayedScaleDownHappen() throws Exception {
        var scaleDownInterval = Duration.ofMinutes(0);
        var metricWindow = Duration.ofMinutes(10);

        var conf = context.getConfiguration();
        conf.set(AutoScalerOptions.SCALE_DOWN_INTERVAL, scaleDownInterval);
        conf.set(AutoScalerOptions.METRICS_WINDOW, metricWindow);

        var records = 0L;
        for (int i = 2; i <= 11; i++) {
            now = now.plus(Duration.ofMinutes(1));
            setClocksTo(now);
            records += 500;
            var finalRecords = records;
            metricsCollector.updateMetrics(
                    source,
                    m -> m.setNumRecordsIn(finalRecords),
                    m -> m.setNumRecordsOut(finalRecords));
            metricsCollector.updateMetrics(sink, m -> m.setNumRecordsIn(finalRecords));

            autoscaler.scale(context);
            assertCollectedMetricsSize(i);

            assertThat(getCurrentMetricValue(source, PARALLELISM)).isEqualTo(100.0);
            assertThat(getCurrentMetricValue(sink, PARALLELISM)).isEqualTo(200.0);
            if (i == 11) {
                // metric window is full
                assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM)).isEqualTo(100.0);
                assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM)).isEqualTo(25.0);
            } else {
                assertThat(getCurrentMetricValue(source, RECOMMENDED_PARALLELISM)).isNull();
                assertThat(getCurrentMetricValue(sink, RECOMMENDED_PARALLELISM)).isNull();
                assertThat(scalingRealizer.events).isEmpty();
            }
        }
    }

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
}
