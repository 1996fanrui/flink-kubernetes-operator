/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.JobPlanInfo;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.util.RestMapperUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RestMapperUtilsTest}. */
class RestMapperUtilsTest {

    private static final String JOB_DETAILS_INFO_MISS_PRIMITIVE_PROPERTIES =
            "{\n"
                    + "  \"jid\": \"e43e5e8b155d969a29c5281c0a82269a\",\n"
                    + "  \"name\": \"foobar\",\n"
                    + "  \"isStoppable\": false,\n"
                    + "  \"state\": \"RUNNING\",\n"
                    + "  \"start-time\": 1710236555559,\n"
                    + "  \"end-time\": -1,\n"
                    + "  \"duration\": 59889,\n"
                    + "  \"maxParallelism\": 24,\n"
                    + "  \"now\": 1710236615448,\n"
                    + "  \"timestamps\": {\n"
                    + "    \"FAILING\": 0,\n"
                    + "    \"FINISHED\": 0,\n"
                    + "    \"SUSPENDED\": 0,\n"
                    + "    \"FAILED\": 0,\n"
                    + "    \"CANCELED\": 0,\n"
                    + "    \"CREATED\": 1710236556167,\n"
                    + "    \"INITIALIZING\": 1710236555559,\n"
                    + "    \"RECONCILING\": 0,\n"
                    + "    \"CANCELLING\": 0,\n"
                    + "    \"RUNNING\": 1710236557060,\n"
                    + "    \"RESTARTING\": 0\n"
                    + "  },\n"
                    + "  \"vertices\": [\n"
                    + "    {\n"
                    + "      \"id\": \"90bea66de1c231edf33913ecd54406c1\",\n"
                    + "      \"name\": \"Source -> Sink: Unnamed\",\n"
                    + "      \"maxParallelism\": 24,\n"
                    + "      \"parallelism\": 1,\n"
                    + "      \"status\": \"CREATED\",\n"
                    + "      \"start-time\": -1,\n"
                    + "      \"end-time\": -1,\n"
                    + "      \"duration\": -1,\n"
                    + "      \"tasks\": {\n"
                    + "        \"FAILED\": 0,\n"
                    + "        \"SCHEDULED\": 1,\n"
                    + "        \"CANCELING\": 0,\n"
                    + "        \"CANCELED\": 0,\n"
                    + "        \"INITIALIZING\": 0,\n"
                    + "        \"FINISHED\": 0,\n"
                    + "        \"CREATED\": 0,\n"
                    + "        \"DEPLOYING\": 0,\n"
                    + "        \"RUNNING\": 0,\n"
                    + "        \"RECONCILING\": 0\n"
                    + "      },\n"
                    + "      \"metrics\": {\n"
                    + "        \"read-bytes\": 1,\n"
                    + "        \"read-bytes-complete\": false,\n"
                    + "        \"write-bytes\": 2,\n"
                    + "        \"write-bytes-complete\": true,\n"
                    + "        \"read-records\": 3,\n"
                    + "        \"read-records-complete\": false,\n"
                    + "        \"write-records\": 4,\n"
                    + "        \"write-records-complete\": true\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ],\n"
                    + "  \"status-counts\": {\n"
                    + "    \"FAILED\": 0,\n"
                    + "    \"SCHEDULED\": 0,\n"
                    + "    \"CANCELING\": 0,\n"
                    + "    \"CANCELED\": 0,\n"
                    + "    \"INITIALIZING\": 0,\n"
                    + "    \"FINISHED\": 0,\n"
                    + "    \"CREATED\": 2,\n"
                    + "    \"DEPLOYING\": 0,\n"
                    + "    \"RUNNING\": 0,\n"
                    + "    \"RECONCILING\": 0\n"
                    + "  },\n"
                    + "  \"plan\": {}\n"
                    + "}";

    @Test
    void testJobDetailsInfoCompatibleWithMissPrimitiveProperties() throws Exception {
        final HashMap<JobStatus, Long> timestamps = new HashMap<>();
        timestamps.put(JobStatus.FAILING, 0L);
        timestamps.put(JobStatus.FINISHED, 0L);
        timestamps.put(JobStatus.SUSPENDED, 0L);
        timestamps.put(JobStatus.FAILED, 0L);
        timestamps.put(JobStatus.CANCELED, 0L);
        timestamps.put(JobStatus.CREATED, 1710236556167L);
        timestamps.put(JobStatus.INITIALIZING, 1710236555559L);
        timestamps.put(JobStatus.RECONCILING, 0L);
        timestamps.put(JobStatus.CANCELLING, 0L);
        timestamps.put(JobStatus.RUNNING, 1710236557060L);
        timestamps.put(JobStatus.RESTARTING, 0L);

        final Map<ExecutionState, Integer> statusCount =
                Map.of(
                        ExecutionState.FAILED,
                        0,
                        ExecutionState.SCHEDULED,
                        0,
                        ExecutionState.CANCELING,
                        0,
                        ExecutionState.CANCELED,
                        0,
                        ExecutionState.INITIALIZING,
                        0,
                        ExecutionState.FINISHED,
                        0,
                        ExecutionState.CREATED,
                        2,
                        ExecutionState.DEPLOYING,
                        0,
                        ExecutionState.RUNNING,
                        0,
                        ExecutionState.RECONCILING,
                        0);

        final JobDetailsInfo.JobVertexDetailsInfo jobVertexDetailsInfo =
                new JobDetailsInfo.JobVertexDetailsInfo(
                        JobVertexID.fromHexString("90bea66de1c231edf33913ecd54406c1"),
                        "Source -> Sink: Unnamed",
                        24,
                        1,
                        ExecutionState.CREATED,
                        -1L,
                        -1L,
                        -1L,
                        Map.of(
                                ExecutionState.FAILED, 0,
                                ExecutionState.SCHEDULED, 1,
                                ExecutionState.CANCELING, 0,
                                ExecutionState.CANCELED, 0,
                                ExecutionState.INITIALIZING, 0,
                                ExecutionState.FINISHED, 0,
                                ExecutionState.CREATED, 0,
                                ExecutionState.DEPLOYING, 0,
                                ExecutionState.RUNNING, 0,
                                ExecutionState.RECONCILING, 0),
                        // Missed properties are default value.
                        new IOMetricsInfo(1, false, 2, true, 3, false, 4, true, 0, 0, 0));

        final ArrayList<JobDetailsInfo.JobVertexDetailsInfo> jobVertexInfos = new ArrayList<>();
        jobVertexInfos.add(jobVertexDetailsInfo);
        final JobDetailsInfo expected =
                new JobDetailsInfo(
                        JobID.fromHexString("e43e5e8b155d969a29c5281c0a82269a"),
                        "foobar",
                        false,
                        JobStatus.RUNNING,
                        1710236555559L,
                        -1L,
                        59889L,
                        24L,
                        1710236615448L,
                        timestamps,
                        jobVertexInfos,
                        statusCount,
                        new JobPlanInfo.RawJson("{}"));

        final JobDetailsInfo jobDetailsInfo =
                RestMapperUtils.getFlexibleObjectMapper()
                        .readValue(
                                JOB_DETAILS_INFO_MISS_PRIMITIVE_PROPERTIES, JobDetailsInfo.class);

        assertThat(jobDetailsInfo).isEqualTo(expected);
    }
}
