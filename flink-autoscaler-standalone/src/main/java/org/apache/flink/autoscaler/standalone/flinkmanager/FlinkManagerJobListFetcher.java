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

package org.apache.flink.autoscaler.standalone.flinkmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.standalone.JobListFetcher;
import org.apache.flink.autoscaler.standalone.StandaloneAutoscalerExecutor;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationInfoHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;

import com.shopee.di.fm.common.dto.InstanceDTO;
import com.shopee.di.fm.common.enums.JobType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Fetch JobAutoScalerContext based on flink cluster. */
public class FlinkManagerJobListFetcher
        implements JobListFetcher<Long, JobAutoScalerContext<Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(StandaloneAutoscalerExecutor.class);

    private final FMClient fmClient;
    private final Duration restClientTimeout;

    public FlinkManagerJobListFetcher(Duration restClientTimeout) {
        this.fmClient = FMClient.getInstance();
        this.restClientTimeout = restClientTimeout;
    }

    @Override
    public Collection<JobAutoScalerContext<Long>> fetch() throws Exception {
        //        final List<ProjectDTO> projectsList = fmClient.fetchAllProjects();
        //        for (ProjectDTO project : projectsList) {
        //            LOG.info("project name: {}", project.getName());
        //        }

        final long startTime = System.currentTimeMillis();
        final String projectName = "guokuai_test";
        LOG.info("Start fetch project [{}] job list.", projectName);

        final List<InstanceDTO> instances =
                fmClient.getRunningInstances(projectName).stream()
                        // Autoscaler only works for streaming job.
                        .filter(instance -> instance.getJobType() == JobType.STREAMING)
                        .collect(Collectors.toList());

        LOG.info("Project {} has {} running streaming instances.", projectName, instances.size());

        var result = new LinkedList<JobAutoScalerContext<Long>>();
        for (InstanceDTO instance : instances) {
            LOG.info(
                    "instance id :{}, name: {}, appId:{}, jobType: {}, yarnTrackingUrl : {}, trackingUrl : {}",
                    instance.getId(),
                    instance.getApplicationName(),
                    instance.getApplicationId(),
                    instance.getJobType(),
                    instance.getYarnTrackingUrl(),
                    instance.getTrackingUrl());
            var restServerAddress = String.format("http://%s", instance.getTrackingUrl());
            try (var restClusterClient = getRestClient(new Configuration(), restServerAddress)) {
                final Collection<JobStatusMessage> jobStatusMessages =
                        restClusterClient
                                .listJobs()
                                .get(restClientTimeout.toSeconds(), TimeUnit.SECONDS);
                if (jobStatusMessages.size() > 1) {
                    LOG.warn(
                            "App {} has {} jobs, the yarnTrackingUrl is {}",
                            instance.getApplicationId(),
                            jobStatusMessages.size(),
                            instance.getYarnTrackingUrl());
                    continue;
                }
                jobStatusMessages.forEach(
                        jobStatusMessage -> {
                            try {
                                result.add(
                                        generateJobContext(
                                                restClusterClient,
                                                restServerAddress,
                                                instance.getApplicationId(),
                                                jobStatusMessage));
                            } catch (Throwable e) {
                                throw new RuntimeException("generateJobContext throw exception", e);
                            }
                        });
            }
        }
        LOG.info(
                "Project {} generated {} job contexts, it costs {} ms.",
                projectName,
                result.size(),
                (System.currentTimeMillis() - startTime));

        return result;
    }

    private JobAutoScalerContext<Long> generateJobContext(
            RestClusterClient<String> restClusterClient,
            String restServerAddress,
            Long appId,
            JobStatusMessage jobStatusMessage)
            throws Exception {
        var jobId = jobStatusMessage.getJobId();
        var conf = getConfiguration(restClusterClient, jobId);

        return new JobAutoScalerContext<>(
                appId,
                jobId,
                jobStatusMessage.getJobState(),
                conf,
                new UnregisteredMetricsGroup(),
                () -> getRestClient(conf, restServerAddress));
    }

    private Configuration getConfiguration(RestClusterClient<String> restClusterClient, JobID jobId)
            throws Exception {
        var jobParameters = new JobMessageParameters();
        jobParameters.jobPathParameter.resolve(jobId);

        var configurationInfo =
                restClusterClient
                        .sendRequest(
                                ClusterConfigurationInfoHeaders.getInstance(),
                                EmptyMessageParameters.getInstance(),
                                EmptyRequestBody.getInstance())
                        .get(restClientTimeout.toSeconds(), TimeUnit.SECONDS);

        var conf = new Configuration();
        configurationInfo.forEach(entry -> conf.setString(entry.getKey(), entry.getValue()));
        return conf;
    }

    private RestClusterClient<String> getRestClient(Configuration conf, String restServerAddress)
            throws Exception {
        return new RestClusterClient<>(
                conf, "clusterId", (c, e) -> new StandaloneClientHAServices(restServerAddress));
    }
}
