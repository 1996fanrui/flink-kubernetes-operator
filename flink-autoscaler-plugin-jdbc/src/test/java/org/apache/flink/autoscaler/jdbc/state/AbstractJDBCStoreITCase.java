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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The abstract IT case for jdbc store. */
abstract class AbstractJDBCStoreITCase {

    protected abstract JDBCStore getJdbcStore() throws Exception;

    @Test
    void testGetAndPut() throws Exception {
        var jdbcStore = getJdbcStore();
        var jobKey = "aaa";

        var serializedState = jdbcStore.getSerializedState(jobKey, StateType.COLLECTED_METRICS);
        assertThat(serializedState).isEmpty();

        var expectedValue1 = "value1";
        jdbcStore.putSerializedState(jobKey, StateType.COLLECTED_METRICS, expectedValue1);
        jdbcStore.flush(jobKey);

        serializedState = jdbcStore.getSerializedState(jobKey, StateType.COLLECTED_METRICS);
        assertThat(serializedState).hasValue(expectedValue1);

        jdbcStore.removeInfoFromCache(jobKey);

        serializedState = jdbcStore.getSerializedState(jobKey, StateType.COLLECTED_METRICS);
        assertThat(serializedState).hasValue(expectedValue1);
    }
}
