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

package org.apache.flink.autoscaler.jdbc;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;

/** An AutoscalerStateStore which persists its state in JDBC related database. */
@Experimental
public class JDBCAutoScalerStateStore<KEY>
        implements AutoScalerStateStore<KEY, JobAutoScalerContext<KEY>> {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCAutoScalerStateStore.class);

    private final JobKeySerializer<KEY> jobKeySerializer;
    private final JDBCStore jdbcStore;

    protected static final ObjectMapper YAML_MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .registerModule(new AutoScalerSerDeModule());

    public JDBCAutoScalerStateStore(JobKeySerializer<KEY> jobKeySerializer) throws SQLException {
        this.jobKeySerializer = jobKeySerializer;
        this.jdbcStore = new JDBCStore(null);
    }

    @Override
    public void storeScalingHistory(
            JobAutoScalerContext<KEY> jobContext,
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory)
            throws Exception {}

    @Nonnull
    @Override
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(
            JobAutoScalerContext<KEY> jobContext) throws Exception {
        return null;
    }

    @Override
    public void storeScalingTracking(
            JobAutoScalerContext<KEY> jobContext, ScalingTracking scalingTrack) throws Exception {}

    @Override
    public ScalingTracking getScalingTracking(JobAutoScalerContext<KEY> jobContext)
            throws Exception {
        return null;
    }

    @Override
    public void removeScalingHistory(JobAutoScalerContext<KEY> jobContext) throws Exception {}

    @Override
    public void storeCollectedMetrics(
            JobAutoScalerContext<KEY> jobContext, SortedMap<Instant, CollectedMetrics> metrics)
            throws Exception {}

    @Nonnull
    @Override
    public SortedMap<Instant, CollectedMetrics> getCollectedMetrics(
            JobAutoScalerContext<KEY> jobContext) throws Exception {
        return null;
    }

    @Override
    public void removeCollectedMetrics(JobAutoScalerContext<KEY> jobContext) throws Exception {}

    @Override
    public void storeParallelismOverrides(
            JobAutoScalerContext<KEY> jobContext, Map<String, String> parallelismOverrides)
            throws Exception {}

    @Nonnull
    @Override
    public Map<String, String> getParallelismOverrides(JobAutoScalerContext<KEY> jobContext)
            throws Exception {
        return null;
    }

    @Override
    public void removeParallelismOverrides(JobAutoScalerContext<KEY> jobContext) throws Exception {}

    @Override
    public void clearAll(JobAutoScalerContext<KEY> jobContext) throws Exception {}

    @Override
    public void flush(JobAutoScalerContext<KEY> jobContext) throws Exception {}

    @Override
    public void removeInfoFromCache(KEY jobKey) {}
}
