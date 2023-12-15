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

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.Map;
import java.util.SortedMap;

/**
 * An AutoscalerStateStore which persists its state in Kubernetes ConfigMaps.
 */
public class JDBCAutoScalerStateStore<KEY, Context extends JobAutoScalerContext<KEY>>
        implements AutoScalerStateStore<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCAutoScalerStateStore.class);

    private enum StateType {
        SCALING_HISTORY,
        SCALING_TRACKING,
        COLLECTED_METRICS,
        PARALLELISM_OVERRIDES
    }

    protected static final String SCALING_HISTORY_KEY = "scalingHistory";
    protected static final String SCALING_TRACKING_KEY = "scalingTracking";
    protected static final String COLLECTED_METRICS_KEY = "collectedMetrics";
    protected static final String PARALLELISM_OVERRIDES_KEY = "parallelismOverrides";

    @Override
    public void storeScalingHistory(Context jobContext, Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) throws Exception {

    }

    @Nonnull
    @Override
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(Context jobContext) throws Exception {
        return null;
    }

    @Override
    public void storeScalingTracking(Context jobContext, ScalingTracking scalingTrack) throws Exception {

    }

    @Override
    public ScalingTracking getScalingTracking(Context jobContext) throws Exception {
        return null;
    }

    @Override
    public void removeScalingHistory(Context jobContext) throws Exception {

    }

    @Override
    public void storeCollectedMetrics(Context jobContext, SortedMap<Instant, CollectedMetrics> metrics) throws Exception {

    }

    @Nonnull
    @Override
    public SortedMap<Instant, CollectedMetrics> getCollectedMetrics(Context jobContext) throws Exception {
        return null;
    }

    @Override
    public void removeCollectedMetrics(Context jobContext) throws Exception {

    }

    @Override
    public void storeParallelismOverrides(Context jobContext, Map<String, String> parallelismOverrides) throws Exception {

    }

    @Nonnull
    @Override
    public Map<String, String> getParallelismOverrides(Context jobContext) throws Exception {
        return null;
    }

    @Override
    public void removeParallelismOverrides(Context jobContext) throws Exception {

    }

    @Override
    public void clearAll(Context jobContext) throws Exception {

    }

    @Override
    public void flush(Context jobContext) throws Exception {

    }

    @Override
    public void removeInfoFromCache(KEY jobKey) {

    }
}
