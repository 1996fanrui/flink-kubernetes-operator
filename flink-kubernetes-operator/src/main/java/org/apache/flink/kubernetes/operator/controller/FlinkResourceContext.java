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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.api.lifecycle.ResourceLifecycleState;
import org.apache.flink.kubernetes.operator.api.spec.AbstractFlinkSpec;
import org.apache.flink.kubernetes.operator.api.spec.KubernetesDeploymentMode;
import org.apache.flink.kubernetes.operator.autoscaler.JobAutoScalerContext;
import org.apache.flink.kubernetes.operator.metrics.KubernetesResourceMetricGroup;
import org.apache.flink.kubernetes.operator.service.FlinkService;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

/** Context for reconciling a Flink resource. * */
@RequiredArgsConstructor
public abstract class FlinkResourceContext<CR extends AbstractFlinkResource<?, ?>> {

    @Getter private final CR resource;
    @Getter private final Context<?> josdkContext;
    @Getter private final KubernetesResourceMetricGroup resourceMetricGroup;

    private Configuration observeConfig;

    public JobAutoScalerContext<ResourceID, CR> getJobAutoScalerContext() {
        Configuration conf = getObserveConfig();
        JobID jobId = JobID.fromHexString(getResource().getStatus().getJobStatus().getJobId());

        return new JobAutoScalerContext<>(
                getResourceID(),
                jobId,
                getGeneration(),
                isReallyRunning(),
                conf,
                getResourceMetricGroup(),
                () -> getFlinkService().getClusterClient(conf),
                getFlinkService().getFlinkClientTimeout(),
                null,
                resource);
    }

    private boolean isReallyRunning() {
        var status = resource.getStatus();
        return status.getLifecycleState() == ResourceLifecycleState.STABLE
                && status.getJobStatus().getState().equals(JobStatus.RUNNING.name());
    }

    private long getGeneration() {
        return resource.getStatus()
                .getReconciliationStatus()
                .deserializeLastReconciledSpecWithMeta()
                .getMeta()
                .getMetadata()
                .getGeneration();
    }

    @NotNull
    public ResourceID getResourceID() {
        return ResourceID.fromResource(resource);
    }

    /**
     * Get the config that is currently deployed for the resource spec. The returned config may be
     * null in case the resource is not accessible/ready yet.
     *
     * @return Config currently deployed.
     */
    public Configuration getObserveConfig() {
        if (observeConfig != null) {
            return observeConfig;
        }
        return observeConfig = createObserveConfig();
    }

    /**
     * Get Flink configuration object for deploying the given spec using {@link
     * org.apache.flink.kubernetes.operator.reconciler.deployment.AbstractFlinkResourceReconciler#deploy}.
     *
     * @param spec Spec for which the config should be created.
     * @return Deployment configuration.
     */
    public abstract Configuration getDeployConfig(AbstractFlinkSpec spec);

    /**
     * Get the {@link FlinkService} implementation for the current resource.
     *
     * @return Flink service.
     */
    public abstract FlinkService getFlinkService();

    /**
     * Generate the config that is currently deployed for the resource spec.
     *
     * @return Deployed config.
     */
    protected abstract Configuration createObserveConfig();

    /** @return Cluster deployment mode. */
    public abstract KubernetesDeploymentMode getDeploymentMode();
}
