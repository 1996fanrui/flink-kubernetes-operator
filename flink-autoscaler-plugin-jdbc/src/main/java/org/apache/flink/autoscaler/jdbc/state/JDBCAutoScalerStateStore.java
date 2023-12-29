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

package org.apache.flink.autoscaler.jdbc.state;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.ScalingSummary;
import org.apache.flink.autoscaler.ScalingTracking;
import org.apache.flink.autoscaler.metrics.CollectedMetrics;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.apache.flink.autoscaler.jdbc.state.StateType.PARALLELISM_OVERRIDES;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_HISTORY;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_TRACKING;

/** An AutoscalerStateStore which persists its state in JDBC related database. */
@Experimental
public class JDBCAutoScalerStateStore<KEY, Context extends JobAutoScalerContext<KEY>>
        implements AutoScalerStateStore<KEY, Context> {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCAutoScalerStateStore.class);

    private final JDBCStore jdbcStore;

    protected static final ObjectMapper YAML_MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .registerModule(new AutoScalerSerDeModule());

    public JDBCAutoScalerStateStore(Connection conn) {
        this.jdbcStore = new JDBCStore(new JDBCStateInteractor(conn));
    }

    @Override
    public void storeScalingHistory(
            Context jobContext, Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory)
            throws Exception {
        jdbcStore.putSerializedState(
                getSerializeKey(jobContext),
                SCALING_HISTORY,
                serializeScalingHistory(scalingHistory));
    }

    @Nonnull
    @Override
    public Map<JobVertexID, SortedMap<Instant, ScalingSummary>> getScalingHistory(
            Context jobContext) {
        Optional<String> serializedScalingHistory =
                jdbcStore.getSerializedState(getSerializeKey(jobContext), SCALING_HISTORY);
        if (serializedScalingHistory.isEmpty()) {
            return new HashMap<>();
        }
        try {
            return deserializeScalingHistory(serializedScalingHistory.get());
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize scaling history, possibly the format changed. Discarding...",
                    e);
            jdbcStore.removeSerializedState(getSerializeKey(jobContext), SCALING_HISTORY);
            return new HashMap<>();
        }
    }

    @Override
    public void removeScalingHistory(Context jobContext) {
        jdbcStore.removeSerializedState(getSerializeKey(jobContext), SCALING_HISTORY);
    }

    @Override
    public void storeScalingTracking(Context jobContext, ScalingTracking scalingTrack)
            throws Exception {
        jdbcStore.putSerializedState(
                getSerializeKey(jobContext),
                SCALING_TRACKING,
                serializeScalingTracking(scalingTrack));
    }

    @Override
    public ScalingTracking getScalingTracking(Context jobContext) {
        Optional<String> serializedRescalingHistory =
                jdbcStore.getSerializedState(getSerializeKey(jobContext), SCALING_TRACKING);
        if (serializedRescalingHistory.isEmpty()) {
            return new ScalingTracking();
        }
        try {
            return deserializeScalingTracking(serializedRescalingHistory.get());
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize rescaling history, possibly the format changed. Discarding...",
                    e);
            jdbcStore.removeSerializedState(getSerializeKey(jobContext), SCALING_TRACKING);
            return new ScalingTracking();
        }
    }

    @Override
    public void storeCollectedMetrics(
            Context jobContext, SortedMap<Instant, CollectedMetrics> metrics) throws Exception {
        jdbcStore.putSerializedState(
                getSerializeKey(jobContext), COLLECTED_METRICS, serializeEvaluatedMetrics(metrics));
    }

    @Nonnull
    @Override
    public SortedMap<Instant, CollectedMetrics> getCollectedMetrics(Context jobContext) {
        Optional<String> serializedEvaluatedMetricsOpt =
                jdbcStore.getSerializedState(getSerializeKey(jobContext), COLLECTED_METRICS);
        if (serializedEvaluatedMetricsOpt.isEmpty()) {
            return new TreeMap<>();
        }
        try {
            return deserializeEvaluatedMetrics(serializedEvaluatedMetricsOpt.get());
        } catch (JacksonException e) {
            LOG.error(
                    "Could not deserialize metric history, possibly the format changed. Discarding...",
                    e);
            jdbcStore.removeSerializedState(getSerializeKey(jobContext), COLLECTED_METRICS);
            return new TreeMap<>();
        }
    }

    @Override
    public void removeCollectedMetrics(Context jobContext) {
        jdbcStore.removeSerializedState(getSerializeKey(jobContext), COLLECTED_METRICS);
    }

    @Override
    public void storeParallelismOverrides(
            Context jobContext, Map<String, String> parallelismOverrides) {
        jdbcStore.putSerializedState(
                getSerializeKey(jobContext),
                PARALLELISM_OVERRIDES,
                serializeParallelismOverrides(parallelismOverrides));
    }

    @Nonnull
    @Override
    public Map<String, String> getParallelismOverrides(Context jobContext) {
        return jdbcStore
                .getSerializedState(getSerializeKey(jobContext), PARALLELISM_OVERRIDES)
                .map(JDBCAutoScalerStateStore::deserializeParallelismOverrides)
                .orElse(new HashMap<>());
    }

    @Override
    public void removeParallelismOverrides(Context jobContext) {
        jdbcStore.removeSerializedState(getSerializeKey(jobContext), PARALLELISM_OVERRIDES);
    }

    @Override
    public void clearAll(Context jobContext) {
        jdbcStore.clearAll(getSerializeKey(jobContext));
    }

    @Override
    public void flush(Context jobContext) throws Exception {
        jdbcStore.flush(getSerializeKey(jobContext));
    }

    @Override
    public void removeInfoFromCache(KEY jobKey) {
        jdbcStore.removeInfoFromCache(getSerializeKey(jobKey));
    }

    private String getSerializeKey(Context jobContext) {
        return getSerializeKey(jobContext.getJobKey());
    }

    private String getSerializeKey(KEY jobKey) {
        return jobKey.toString();
    }

    // The serialization and deserialization are same with KubernetesAutoScalerStateStore
    protected static String serializeScalingHistory(
            Map<JobVertexID, SortedMap<Instant, ScalingSummary>> scalingHistory) throws Exception {
        return compress(YAML_MAPPER.writeValueAsString(scalingHistory));
    }

    private static Map<JobVertexID, SortedMap<Instant, ScalingSummary>> deserializeScalingHistory(
            String scalingHistory) throws JacksonException {
        return YAML_MAPPER.readValue(decompress(scalingHistory), new TypeReference<>() {});
    }

    protected static String serializeScalingTracking(ScalingTracking scalingTracking)
            throws Exception {
        return compress(YAML_MAPPER.writeValueAsString(scalingTracking));
    }

    private static ScalingTracking deserializeScalingTracking(String scalingTracking)
            throws JacksonException {
        return YAML_MAPPER.readValue(decompress(scalingTracking), new TypeReference<>() {});
    }

    @VisibleForTesting
    protected static String serializeEvaluatedMetrics(
            SortedMap<Instant, CollectedMetrics> evaluatedMetrics) throws Exception {
        return compress(YAML_MAPPER.writeValueAsString(evaluatedMetrics));
    }

    private static SortedMap<Instant, CollectedMetrics> deserializeEvaluatedMetrics(
            String evaluatedMetrics) throws JacksonException {
        return YAML_MAPPER.readValue(decompress(evaluatedMetrics), new TypeReference<>() {});
    }

    private static String serializeParallelismOverrides(Map<String, String> overrides) {
        return ConfigurationUtils.convertValue(overrides, String.class);
    }

    private static Map<String, String> deserializeParallelismOverrides(String overrides) {
        return ConfigurationUtils.convertValue(overrides, Map.class);
    }

    private static String compress(String original) throws IOException {
        ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
        try (var zos = new GZIPOutputStream(rstBao)) {
            zos.write(original.getBytes(StandardCharsets.UTF_8));
        }

        return Base64.getEncoder().encodeToString(rstBao.toByteArray());
    }

    private static String decompress(String compressed) {
        if (compressed == null) {
            return null;
        }
        try {
            byte[] bytes = Base64.getDecoder().decode(compressed);
            try (var zi = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
                return IOUtils.toString(zi, StandardCharsets.UTF_8);
            }
        } catch (Exception e) {
            LOG.warn("Error while decompressing scaling data, treating as uncompressed");
            // Fall back to non-compressed for migration
            return compressed;
        }
    }
}
