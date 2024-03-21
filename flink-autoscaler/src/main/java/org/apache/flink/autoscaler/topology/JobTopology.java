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

package org.apache.flink.autoscaler.topology;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Structure representing information about the jobgraph that is relevant for scaling. */
@ToString
@EqualsAndHashCode
public class JobTopology {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Whether all tasks are running, it's different with JobStatus.RUNNING. The JobStatus will be
     * RUNNING once job starts schedule, so it doesn't mean all tasks are running. Especially, when
     * the resource isn't enough or job recovers from large state.
     */
    @Getter private final boolean isRunning;

    @Getter private final Map<JobVertexID, VertexInfo> vertexInfos;
    @Getter private final Set<JobVertexID> finishedVertices;
    @Getter private final List<JobVertexID> verticesInTopologicalOrder;

    public JobTopology(Collection<VertexInfo> vertexInfo) {
        this(new HashSet<>(vertexInfo));
    }

    public JobTopology(VertexInfo... vertexInfo) {
        this(Set.of(vertexInfo));
    }

    public JobTopology(Set<VertexInfo> vertexInfo) {
        this.isRunning = jobIsRunning(vertexInfo);

        Map<JobVertexID, Map<JobVertexID, ShipStrategy>> vertexOutputs = new HashMap<>();
        vertexInfos =
                ImmutableMap.copyOf(
                        vertexInfo.stream().collect(Collectors.toMap(VertexInfo::getId, v -> v)));

        var finishedVertices = ImmutableSet.<JobVertexID>builder();

        vertexInfo.forEach(
                info -> {
                    var vertexId = info.getId();

                    vertexOutputs.computeIfAbsent(vertexId, id -> new HashMap<>());
                    info.getInputs()
                            .forEach(
                                    (inputId, shipStrategy) ->
                                            vertexOutputs
                                                    .computeIfAbsent(inputId, id -> new HashMap<>())
                                                    .put(vertexId, shipStrategy));
                    if (info.isFinished()) {
                        finishedVertices.add(vertexId);
                    }
                });
        vertexOutputs.forEach((v, outputs) -> vertexInfos.get(v).setOutputs(outputs));

        this.finishedVertices = finishedVertices.build();
        this.verticesInTopologicalOrder = returnVerticesInTopologicalOrder();
    }

    public VertexInfo get(JobVertexID jvi) {
        return vertexInfos.get(jvi);
    }

    public boolean isSource(JobVertexID jobVertexID) {
        return get(jobVertexID).getInputs().isEmpty();
    }

    public void updateMaxParallelism(JobVertexID vertexID, int maxParallelism) {
        get(vertexID).updateMaxParallelism(maxParallelism);
    }

    private boolean jobIsRunning(Set<VertexInfo> vertexInfos) {
        // All vertices are running or finished, and at least one vertex is running.
        return vertexInfos.stream()
                        .allMatch(vertexInfo -> vertexInfo.isRunning() || vertexInfo.isFinished())
                && vertexInfos.stream().anyMatch(VertexInfo::isRunning);
    }

    private List<JobVertexID> returnVerticesInTopologicalOrder() {
        List<JobVertexID> sorted = new ArrayList<>(vertexInfos.size());

        Map<JobVertexID, List<JobVertexID>> remainingInputs = new HashMap<>(vertexInfos.size());
        vertexInfos.forEach(
                (id, v) -> remainingInputs.put(id, new ArrayList<>(v.getInputs().keySet())));

        while (!remainingInputs.isEmpty()) {
            List<JobVertexID> verticesWithZeroIndegree = new ArrayList<>();
            remainingInputs.forEach(
                    (v, inputs) -> {
                        if (inputs.isEmpty()) {
                            verticesWithZeroIndegree.add(v);
                        }
                    });

            verticesWithZeroIndegree.forEach(
                    v -> {
                        remainingInputs.remove(v);
                        vertexInfos
                                .get(v)
                                .getOutputs()
                                .keySet()
                                .forEach(o -> remainingInputs.get(o).remove(v));
                    });

            sorted.addAll(verticesWithZeroIndegree);
        }
        return sorted;
    }

    public static JobTopology fromJsonPlan(
            String jsonPlan,
            Map<JobVertexID, Integer> maxParallelismMap,
            Map<JobVertexID, Integer> finishedTasks,
            Map<JobVertexID, Integer> runningTasks,
            Map<JobVertexID, IOMetrics> metrics)
            throws JsonProcessingException {
        ObjectNode plan = objectMapper.readValue(jsonPlan, ObjectNode.class);
        ArrayNode nodes = (ArrayNode) plan.get("nodes");

        var vertexInfo = new HashSet<VertexInfo>();

        for (JsonNode node : nodes) {
            var vertexId = JobVertexID.fromHexString(node.get("id").asText());
            var inputs = new HashMap<JobVertexID, ShipStrategy>();
            var ioMetrics = metrics.get(vertexId);
            var parallelism = node.get("parallelism").asInt();
            vertexInfo.add(
                    new VertexInfo(
                            vertexId,
                            inputs,
                            parallelism,
                            maxParallelismMap.get(vertexId),
                            finishedTasks.get(vertexId),
                            runningTasks.get(vertexId),
                            parallelism == finishedTasks.get(vertexId)
                                    ? IOMetrics.FINISHED_METRICS
                                    : ioMetrics));
            if (node.has("inputs")) {
                for (JsonNode input : node.get("inputs")) {
                    inputs.put(
                            JobVertexID.fromHexString(input.get("id").asText()),
                            ShipStrategy.of(input.get("ship_strategy").asText()));
                }
            }
        }

        return new JobTopology(vertexInfo);
    }
}
