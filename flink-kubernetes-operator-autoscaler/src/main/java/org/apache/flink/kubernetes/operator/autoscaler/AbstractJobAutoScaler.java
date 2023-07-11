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

package org.apache.flink.kubernetes.operator.autoscaler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.reconciler.deployment.JobAutoScaler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.kubernetes.operator.autoscaler.config.AutoScalerOptions.AUTOSCALER_ENABLED;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.PARALLELISM;
import static org.apache.flink.kubernetes.operator.autoscaler.metrics.ScalingMetric.RECOMMENDED_PARALLELISM;

/** Application and SessionJob autoscaler. */
public abstract class AbstractJobAutoScaler<KEY> implements JobAutoScaler<KEY> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobAutoScaler.class);

    private final KubernetesClient kubernetesClient;
    private final ScalingMetricCollector metricsCollector;
    private final ScalingMetricEvaluator evaluator;
    private final ScalingExecutor scalingExecutor;
    private final EventRecorder eventRecorder;

    @VisibleForTesting
    final Map<KEY, Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>>
            lastEvaluatedMetrics = new ConcurrentHashMap<>();

    @VisibleForTesting
    final Map<KEY, AutoscalerFlinkMetrics> flinkMetrics = new ConcurrentHashMap<>();

    public AbstractJobAutoScaler(
            KubernetesClient kubernetesClient,
            ScalingMetricCollector metricsCollector,
            ScalingMetricEvaluator evaluator,
            ScalingExecutor scalingExecutor,
            EventRecorder eventRecorder) {
        this.kubernetesClient = kubernetesClient;
        this.metricsCollector = metricsCollector;
        this.evaluator = evaluator;
        this.scalingExecutor = scalingExecutor;
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void cleanup(JobAutoScalerContext<KEY> context) {
        LOG.info("Cleaning up autoscaling meta data");
        metricsCollector.cleanup(cr);
        lastEvaluatedMetrics.remove(context.getJobKey());
        flinkMetrics.remove(context.getJobKey());
    }

    @Override
    public boolean scale(JobAutoScalerContext<KEY> context) {

        var conf = context.getConf();
        var resource = ctx.getResource();
        var jobKey = context.getJobKey();
        var flinkMetrics = getOrInitAutoscalerFlinkMetrics(context, jobKey);
        Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> evaluatedMetrics = null;

        try {

            if (resource.getSpec().getJob() == null || !conf.getBoolean(AUTOSCALER_ENABLED)) {
                LOG.debug("Job autoscaler is disabled");
                return false;
            }

            // Initialize metrics only if autoscaler is enabled

            var status = resource.getStatus();
            if (status.getLifecycleState() != ResourceLifecycleState.STABLE
                    || !status.getJobStatus().getState().equals(JobStatus.RUNNING.name())) {
                LOG.info("Job autoscaler is waiting for RUNNING job state");
                lastEvaluatedMetrics.remove(jobKey);
                return false;
            }

            var autoScalerInfo = AutoScalerInfo.forResource(resource, kubernetesClient);

            var collectedMetrics =
                    metricsCollector.updateMetrics(
                            resource, autoScalerInfo, ctx.getFlinkService(), conf);

            if (collectedMetrics.getMetricHistory().isEmpty()) {
                autoScalerInfo.replaceInKubernetes(kubernetesClient);
                return false;
            }

            LOG.debug("Collected metrics: {}", collectedMetrics);
            evaluatedMetrics = evaluator.evaluate(conf, collectedMetrics);
            LOG.debug("Evaluated metrics: {}", evaluatedMetrics);
            initRecommendedParallelism(evaluatedMetrics);

            if (!collectedMetrics.isFullyCollected()) {
                // We have done an upfront evaluation, but we are not ready for scaling.
                resetRecommendedParallelism(evaluatedMetrics);
                autoScalerInfo.replaceInKubernetes(kubernetesClient);
                return false;
            }

            var specAdjusted =
                    scalingExecutor.scaleResource(resource, autoScalerInfo, conf, evaluatedMetrics);

            if (specAdjusted) {
                flinkMetrics.numScalings.inc();
            } else {
                flinkMetrics.numBalanced.inc();
            }

            autoScalerInfo.replaceInKubernetes(kubernetesClient);
            return specAdjusted;
        } catch (Throwable e) {
            LOG.error("Error while scaling resource", e);
            flinkMetrics.numErrors.inc();
            eventRecorder.triggerEvent(
                    resource,
                    EventRecorder.Type.Warning,
                    EventRecorder.Reason.AutoscalerError,
                    EventRecorder.Component.Operator,
                    e.getMessage());
            return false;
        } finally {
            if (evaluatedMetrics != null) {
                lastEvaluatedMetrics.put(jobKey, evaluatedMetrics);
                flinkMetrics.registerScalingMetrics(() -> lastEvaluatedMetrics.get(jobKey));
            }
        }
    }

    private AutoscalerFlinkMetrics getOrInitAutoscalerFlinkMetrics(
            JobAutoScalerContext<KEY> context, KEY jobKey) {
        return this.flinkMetrics.computeIfAbsent(
                jobKey,
                id ->
                        new AutoscalerFlinkMetrics(
                                context.getMetricGroup().addGroup("AutoScaler")));
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
