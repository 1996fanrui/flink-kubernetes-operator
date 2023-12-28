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

import org.apache.flink.autoscaler.jdbc.testutils.databases.DatabaseTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_HISTORY;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_TRACKING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The abstract IT case for {@link JDBCStore}. */
public abstract class AbstractJDBCStoreITCase implements DatabaseTest {

    private static final String DEFAULT_JOB_KEY = "jobKey";
    private CountableJDBCStateInteractor jdbcStateInteractor;
    private JDBCStore jdbcStore;

    @BeforeEach
    void beforeEach() throws Exception {
        this.jdbcStateInteractor = new CountableJDBCStateInteractor(getConnection());
        this.jdbcStore = new JDBCStore(jdbcStateInteractor);
    }

    @Test
    void testCaching() throws Exception {
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";

        jdbcStateInteractor.assertCountableJDBCInteractor(0, 0, 0, 0);

        // Query from database.
        jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS);
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        // The rest of state types of same job key shouldn't query database.
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        //  Putting does not go to database, unless flushing.
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY, value2);
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        // Flush together! Create counter is one.
        jdbcStore.flush(DEFAULT_JOB_KEY);
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 1);

        // Get
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value2);
        var job2 = "job2";
        assertThat(jdbcStore.getSerializedState(job2, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(2, 0, 0, 1);
        assertThat(jdbcStore.getSerializedState(job2, SCALING_HISTORY)).isEmpty();

        // Put and flush state for job2
        jdbcStore.putSerializedState(job2, SCALING_TRACKING, value3);
        jdbcStore.flush(job2);
        jdbcStateInteractor.assertCountableJDBCInteractor(2, 0, 0, 2);

        // Build the new JDBCStore
        var newJdbcStore = new JDBCStore(jdbcStateInteractor);
        assertThat(newJdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .hasValue(value1);
        assertThat(newJdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY))
                .hasValue(value2);
        jdbcStateInteractor.assertCountableJDBCInteractor(3, 0, 0, 2);

        assertThat(newJdbcStore.getSerializedState(job2, SCALING_TRACKING)).hasValue(value3);
        jdbcStateInteractor.assertCountableJDBCInteractor(4, 0, 0, 2);

        // Removing the data from cache and query from database again.
        newJdbcStore.removeInfoFromCache(job2);
        assertThat(newJdbcStore.getSerializedState(job2, SCALING_TRACKING)).hasValue(value3);
        jdbcStateInteractor.assertCountableJDBCInteractor(5, 0, 0, 2);
    }

    @Test
    void testDeleting() throws Exception {
        var value1 = "value1";

        jdbcStateInteractor.assertCountableJDBCInteractor(0, 0, 0, 0);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        // Get from cache, and it shouldn't exist in database.
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .hasValue(value1);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        // Deleting before flushing
        jdbcStore.removeSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        // Flush method shouldn't flush any data.
        jdbcStore.flush(DEFAULT_JOB_KEY);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        // Put and flush data to database.
        var value2 = "value2";
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value2);
        jdbcStore.flush(DEFAULT_JOB_KEY);
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value2);
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 1);

        // Deleting after flushing, data is deleted in cache, but it still exists in database.
        jdbcStore.removeSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).hasValue(value2);

        // Flushing
        jdbcStore.flush(DEFAULT_JOB_KEY);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 1, 0, 1);

        // Get from database for a new JDBC Store.
        var newJdbcStore = new JDBCStore(jdbcStateInteractor);
        assertThat(newJdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
    }

    @Test
    void testErrorHandlingDuringFlush() throws Exception {
        var value1 = "value1";
        var value2 = "value2";
        jdbcStateInteractor.assertCountableJDBCInteractor(0, 0, 0, 0);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();

        // Modify the database directly.
        var tmpJdbcInteractor = new JDBCStateInteractor(getConnection());
        tmpJdbcInteractor.createData(
                DEFAULT_JOB_KEY, List.of(COLLECTED_METRICS), Map.of(COLLECTED_METRICS, value1));
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).hasValue(value1);

        // Cache cannot read data of database.
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 0);

        // Create it with SQLException due to the data has already existed in database.
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value2);
        assertThatThrownBy(() -> jdbcStore.flush(DEFAULT_JOB_KEY))
                .hasCauseInstanceOf(SQLException.class);
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 1);

        // Get normally.
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .hasValue(value1);
        jdbcStateInteractor.assertCountableJDBCInteractor(2, 0, 0, 1);
    }

    @Test
    void testErrorHandlingDuringQuery() throws Exception {
        var value1 = "value1";
        final var expectedException = new RuntimeException("Database isn't stable.");

        var exceptionableJdbcStateInteractor =
                new CountableJDBCStateInteractor(getConnection()) {
                    private final AtomicBoolean isFirst = new AtomicBoolean(true);

                    @Override
                    public Map<StateType, String> queryData(String jobKey) throws Exception {
                        if (isFirst.get()) {
                            isFirst.set(false);
                            throw expectedException;
                        }
                        return super.queryData(jobKey);
                    }
                };

        var exceptionableJdbcStore = new JDBCStore(exceptionableJdbcStateInteractor);

        // First get will fail.
        jdbcStateInteractor.assertCountableJDBCInteractor(0, 0, 0, 0);
        assertThatThrownBy(
                        () ->
                                exceptionableJdbcStore.getSerializedState(
                                        DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .rootCause()
                .isSameAs(expectedException);

        // It's recovered.
        assertThat(exceptionableJdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS))
                .isEmpty();
        exceptionableJdbcStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        exceptionableJdbcStore.flush(DEFAULT_JOB_KEY);
        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
    }

    @Test
    void testDiscardAllState() throws Exception {
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";

        // Put and flush all state types first.
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS, value1);
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY, value2);
        jdbcStore.putSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING, value3);
        jdbcStore.flush(DEFAULT_JOB_KEY);
        jdbcStateInteractor.assertCountableJDBCInteractor(1, 0, 0, 1);

        assertStateValueForCacheAndDatabase(COLLECTED_METRICS, value1);
        assertStateValueForCacheAndDatabase(SCALING_HISTORY, value2);
        assertStateValueForCacheAndDatabase(SCALING_TRACKING, value3);

        // Clear all in cache.
        jdbcStore.clearAll(DEFAULT_JOB_KEY);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).hasValue(value1);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_HISTORY)).hasValue(value2);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_TRACKING)).hasValue(value3);

        // Flush!
        jdbcStore.flush(DEFAULT_JOB_KEY);
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, COLLECTED_METRICS)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_HISTORY)).isEmpty();
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, SCALING_TRACKING)).isEmpty();
    }

    private void assertStateValueForCacheAndDatabase(StateType stateType, String expectedValue)
            throws Exception {
        assertThat(jdbcStore.getSerializedState(DEFAULT_JOB_KEY, stateType))
                .hasValue(expectedValue);
        assertThat(getValueFromDatabase(DEFAULT_JOB_KEY, stateType)).hasValue(expectedValue);
    }

    private Optional<String> getValueFromDatabase(String jobKey, StateType stateType)
            throws Exception {
        var jdbcInteractor = new JDBCStateInteractor(getConnection());
        return Optional.ofNullable(jdbcInteractor.queryData(jobKey).get(stateType));
    }
}
