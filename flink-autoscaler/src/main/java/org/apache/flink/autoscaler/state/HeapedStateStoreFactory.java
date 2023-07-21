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

package org.apache.flink.autoscaler.state;

import java.util.Optional;

/** The factory for heaped state store. */
public class HeapedStateStoreFactory implements AutoScalerStateStoreFactory {

    @Override
    public Optional<AutoScalerStateStore> get() {
        return Optional.of(getOrCreate());
    }

    public AutoScalerStateStore getOrCreate() {
        return new HeapedAutoScalerStateStore();
    }
}
