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

import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** All delayed scale down requests. */
public class DelayedScaleDown {

    /** The scale down info for vertex. */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VertexInfo {
        private Instant firstTriggerTime;
        private int maxRecommendedParallelism;
    }

    @Getter private final Map<JobVertexID, VertexInfo> delayedVertices;

    // Have any scale down request been updated? It doesn't need to be stored, it is only used to
    // determine whether DelayedScaleDown needs to be stored.
    @JsonIgnore @Getter private boolean updated = false;

    public DelayedScaleDown() {
        this.delayedVertices = new HashMap<>();
    }

    Optional<Instant> getFirstTriggerTimeForVertex(JobVertexID vertex) {
        return Optional.ofNullable(delayedVertices.get(vertex))
                .map(VertexInfo::getFirstTriggerTime);
    }

    int getMaxRecommendedParallelism(JobVertexID vertex) {
        var vertexInfo = delayedVertices.get(vertex);
        checkState(
                vertexInfo != null,
                "The scale down is not triggered for vertex [%s], so maxRecommendedParallelism cannot be got.",
                vertex);
        return delayedVertices.get(vertex).getMaxRecommendedParallelism();
    }

    public void triggerScaleDown(JobVertexID vertex, Instant triggerTime, int parallelism) {
        checkState(
                !delayedVertices.containsKey(vertex),
                "The scale down has been triggered for vertex [%s].",
                vertex);
        delayedVertices.put(vertex, new DelayedScaleDown.VertexInfo(triggerTime, parallelism));
        updated = true;
    }

    void updateParallelism(JobVertexID vertex, int parallelism) {
        var vertexInfo = delayedVertices.get(vertex);
        checkState(
                vertexInfo != null,
                "The scale down is not triggered for vertex [%s], so it cannot be updated.",
                vertex);
        if (parallelism <= vertexInfo.getMaxRecommendedParallelism()) {
            return;
        }
        vertexInfo.setMaxRecommendedParallelism(parallelism);
        updated = true;
    }

    void clearVertex(JobVertexID vertex) {
        VertexInfo removed = delayedVertices.remove(vertex);
        if (removed != null) {
            updated = true;
        }
    }

    void clearAll() {
        if (delayedVertices.isEmpty()) {
            return;
        }
        delayedVertices.clear();
        updated = true;
    }
}
