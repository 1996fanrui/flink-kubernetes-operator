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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.event.AutoScalerHandler;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;

/** Application and SessionJob autoscaler. */
public class JobAutoScalerImpl<KEY, INFO> implements JobAutoScaler<KEY, INFO> {

    private static final Logger LOG = LoggerFactory.getLogger(JobAutoScalerImpl.class);

    private final ScalingMetricCollector<KEY, INFO> metricsCollector;
    private final ScalingMetricEvaluator evaluator;
    private final ScalingExecutor<KEY, INFO> scalingExecutor;
    private final AutoScalerHandler<KEY, INFO> autoScalerHandler;

    @VisibleForTesting
    final Map<KEY, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
            lastEvaluatedMetrics = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<KEY, AutoscalerFlinkMetrics> flinkMetrics = new ConcurrentHashMap<>();

    public JobAutoScalerImpl(
            ScalingMetricCollector<KEY, INFO> metricsCollector,
            ScalingMetricEvaluator evaluator,
            ScalingExecutor<KEY, INFO> scalingExecutor,
            AutoScalerHandler<KEY, INFO> autoScalerHandler) {
        this.metricsCollector = metricsCollector;
        this.evaluator = evaluator;
        this.scalingExecutor = scalingExecutor;
        this.autoScalerHandler = autoScalerHandler;
    }

    @Override
    public void cleanup(KEY jobKey) {
        LOG.info("Cleaning up autoscaling meta data");
        metricsCollector.cleanup(jobKey);
        lastEvaluatedMetrics.remove(jobKey);
        flinkMetrics.remove(jobKey);
    }

    @Override
    public boolean scale(JobAutoScalerContext<KEY, INFO> context) {

        var conf = context.getConf();
        var jobKey = context.getJobKey();
        var flinkMetrics = getOrInitAutoscalerFlinkMetrics(context, jobKey);
        Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics = null;

        try {

            if (!conf.getBoolean(AUTOSCALER_ENABLED)) {
                LOG.debug("Job autoscaler is disabled");
                return false;
            }

            // Initialize metrics only if autoscaler is enabled

            if (!context.isRunning()) {
                LOG.info("Job autoscaler is waiting for RUNNING job state");
                lastEvaluatedMetrics.remove(context.getJobKey());
                return false;
            }

            var autoScalerInfo = new AutoScalerInfo(context.getStateStore());

            var collectedMetrics = metricsCollector.updateMetrics(context, autoScalerInfo);

            if (collectedMetrics.getMetricHistory().isEmpty()) {
                autoScalerInfo.persistState();
                return false;
            }

            LOG.debug("Collected metrics: {}", collectedMetrics);
            evaluatedMetrics = evaluator.evaluate(conf, collectedMetrics);
            LOG.debug("Evaluated metrics: {}", evaluatedMetrics);
            initRecommendedParallelism(evaluatedMetrics);

            if (!collectedMetrics.isFullyCollected()) {
                // We have done an upfront evaluation, but we are not ready for scaling.
                resetRecommendedParallelism(evaluatedMetrics);
                autoScalerInfo.persistState();
                return false;
            }

            var specAdjusted =
                    scalingExecutor.scaleResource(context, autoScalerInfo, conf, evaluatedMetrics);

            if (specAdjusted) {
                flinkMetrics.numScalings.inc();
            } else {
                flinkMetrics.numBalanced.inc();
            }

            autoScalerInfo.persistState();
            return specAdjusted;
        } catch (Throwable e) {
            LOG.error("Error while scaling resource", e);
            flinkMetrics.numErrors.inc();
            autoScalerHandler.handlerScalingError(context, e.getMessage());
            return false;
        } finally {
            if (evaluatedMetrics != null) {
                lastEvaluatedMetrics.put(jobKey, evaluatedMetrics);
                flinkMetrics.registerScalingMetrics(() -> lastEvaluatedMetrics.get(jobKey));
            }
        }
    }

    private AutoscalerFlinkMetrics getOrInitAutoscalerFlinkMetrics(
            JobAutoScalerContext<KEY, INFO> context, KEY jobKey) {
        return this.flinkMetrics.computeIfAbsent(
                jobKey,
                id -> new AutoscalerFlinkMetrics(context.getMetricGroup().addGroup("AutoScaler")));
    }

    private void initRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        evaluatedMetrics.forEach(
                (jobVertexID, evaluatedScalingMetricMap) ->
                        evaluatedScalingMetricMap.put(
                                RECOMMENDED_PARALLELISM,
                                evaluatedScalingMetricMap.get(PARALLELISM)));
    }

    private void resetRecommendedParallelism(
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics) {
        evaluatedMetrics.forEach(
                (jobVertexID, evaluatedScalingMetricMap) ->
                        evaluatedScalingMetricMap.put(RECOMMENDED_PARALLELISM, null));
    }
}
