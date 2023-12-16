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

import static org.apache.flink.util.Preconditions.checkArgument;

/** The state type. */
public enum StateType {
    SCALING_HISTORY,
    SCALING_TRACKING,
    COLLECTED_METRICS,
    PARALLELISM_OVERRIDES;

    public static StateType valueOf(int ordinal) {
        checkArgument(
                ordinal >= 0 && ordinal < values().length, "It's a out-of-bounded ordinal index.");
        return values()[ordinal];
    }
}
