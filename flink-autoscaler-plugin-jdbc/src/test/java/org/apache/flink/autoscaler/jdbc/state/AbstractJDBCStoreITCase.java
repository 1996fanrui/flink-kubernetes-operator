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

import java.sql.Connection;
import java.util.Optional;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.assertj.core.api.Assertions.assertThat;

/** The abstract IT case for {@link JDBCStore}. */
abstract class AbstractJDBCStoreITCase {

    protected abstract Connection getConnection() throws Exception;

    @Test
    void testCreateAndGet() throws Exception {
        var countableJDBCInteractor = new CountableJDBCInteractor(getConnection());
        var jdbcStore = new JDBCStore(countableJDBCInteractor);
        var jobKey = "aaa";
        var expectedValue1 = "value1";

        assertCountableJDBCInteractor(countableJDBCInteractor, 0, 0, 0, 0);
        assertThat(jdbcStore.getSerializedState(jobKey, COLLECTED_METRICS)).isEmpty();
        assertCountableJDBCInteractor(countableJDBCInteractor, 1, 0, 0, 0);

        // Get from cache, and it shouldn't exist in database.
        jdbcStore.putSerializedState(jobKey, COLLECTED_METRICS, expectedValue1);
        assertThat(jdbcStore.getSerializedState(jobKey, COLLECTED_METRICS))
                .hasValue(expectedValue1);
        assertThat(getValueFromDatabase(jobKey, COLLECTED_METRICS)).isEmpty();
        assertCountableJDBCInteractor(countableJDBCInteractor, 1, 0, 0, 0);

        // Get from cache after flushing, and it should exist in database.
        jdbcStore.flush(jobKey);
        assertStateValueForCacheAndDatabase(jdbcStore, jobKey, COLLECTED_METRICS, expectedValue1);
        assertCountableJDBCInteractor(countableJDBCInteractor, 1, 0, 0, 1);

        // Get from database for a old JDBC Store.
        jdbcStore.removeInfoFromCache(jobKey);
        assertCountableJDBCInteractor(countableJDBCInteractor, 1, 0, 0, 1);
        assertStateValueForCacheAndDatabase(jdbcStore, jobKey, COLLECTED_METRICS, expectedValue1);
        assertCountableJDBCInteractor(countableJDBCInteractor, 2, 0, 0, 1);

        // Get from database for a new JDBC Store.
        var newJdbcStore = new JDBCStore(countableJDBCInteractor);
        assertStateValueForCacheAndDatabase(
                newJdbcStore, jobKey, COLLECTED_METRICS, expectedValue1);
        assertCountableJDBCInteractor(countableJDBCInteractor, 3, 0, 0, 1);
    }

    @Test
    void testUpdate() throws Exception {
        var countableJDBCInteractor = new CountableJDBCInteractor(getConnection());
        var jdbcStore = new JDBCStore(countableJDBCInteractor);
        var jobKey = "aaa";
        var expectedValue1 = "value1";
        // TODO
    }

    @Test
    void testMultipleStateTypes() throws Exception {
        var countableJDBCInteractor = new CountableJDBCInteractor(getConnection());
        var jdbcStore = new JDBCStore(countableJDBCInteractor);
        var jobKey = "aaa";
        var expectedValue1 = "value1";
        // TODO
    }

    @Test
    void testMultipleJobKeys() throws Exception {
        var countableJDBCInteractor = new CountableJDBCInteractor(getConnection());
        var jdbcStore = new JDBCStore(countableJDBCInteractor);
        var jobKey = "aaa";
        var expectedValue1 = "value1";
        // TODO
    }

    private void assertCountableJDBCInteractor(
            CountableJDBCInteractor jdbcInteractor,
            long expectedQueryCounter,
            long expectedDeleteCounter,
            long expectedUpdateCounter,
            long expectedCreateCounter) {
        assertThat(jdbcInteractor.getQueryCounter()).isEqualTo(expectedQueryCounter);
        assertThat(jdbcInteractor.getDeleteCounter()).isEqualTo(expectedDeleteCounter);
        assertThat(jdbcInteractor.getUpdateCounter()).isEqualTo(expectedUpdateCounter);
        assertThat(jdbcInteractor.getCreateCounter()).isEqualTo(expectedCreateCounter);
    }

    private void assertStateValueForCacheAndDatabase(
            JDBCStore jdbcStore, String jobKey, StateType stateType, String expectedValue)
            throws Exception {
        assertThat(jdbcStore.getSerializedState(jobKey, stateType)).hasValue(expectedValue);
        assertThat(getValueFromDatabase(jobKey, stateType)).hasValue(expectedValue);
    }

    private Optional<String> getValueFromDatabase(String jobKey, StateType stateType)
            throws Exception {
        var jdbcInteractor = new JDBCInteractor(getConnection());
        return Optional.ofNullable(jdbcInteractor.queryData(jobKey).get(stateType));
    }
}
