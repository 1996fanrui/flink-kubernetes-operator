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

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** The view of job state. */
@NotThreadSafe
public class JobStateView {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCAutoScalerStateStore.class);

    private static final PutStateTransitioner PUT_STATE_TRANSITIONER = new PutStateTransitioner();
    private static final DeleteStateTransitioner DELETE_STATE_TRANSITIONER =
            new DeleteStateTransitioner();
    private static final FlushStateTransitioner FLUSH_STATE_TRANSITIONER =
            new FlushStateTransitioner();

    private final Connection conn;

    private final String jobKey;
    private final HashMap<StateType, String> data;

    /**
     * The state is maintained for each state type, which means that part of state types of current
     * job are stored in the database, but the rest of the state types may have been created in the
     * database.
     */
    private final HashMap<StateType, State> states;

    public JobStateView(Connection conn, String jobKey) throws Exception {
        this.conn = conn;
        this.jobKey = jobKey;
        this.data = queryFromDatabase(jobKey);
        this.states = generateStates(this.data);
    }

    private HashMap<StateType, State> generateStates(HashMap<StateType, String> data) {
        final var states = new HashMap<StateType, State>();
        for (StateType stateType : StateType.values()) {
            if (data.containsKey(stateType)) {
                states.put(stateType, State.UP_TO_DATE);
            } else {
                states.put(stateType, State.NOT_NEEDED);
            }
        }
        return states;
    }

    public String get(StateType stateType) {
        return data.get(stateType);
    }

    public void put(StateType stateType, String value) {
        data.put(stateType, value);
        updateState(stateType, PUT_STATE_TRANSITIONER);
    }

    public void removeKey(StateType stateType) {
        var oldKey = data.remove(stateType);
        if (oldKey == null) {
            return;
        }
        updateState(stateType, DELETE_STATE_TRANSITIONER);
    }

    public void clear() {
        if (data.isEmpty()) {
            return;
        }
        var iterator = data.keySet().iterator();
        while (iterator.hasNext()) {
            var stateType = iterator.next();
            iterator.remove();
            updateState(stateType, DELETE_STATE_TRANSITIONER);
        }
    }

    public void flush() throws Exception {
        if (states.values().stream().noneMatch(State::isNeedFlush)) {
            // No any state needs to be flushed.
            return;
        }

        // Build the data that need to be flushed.
        var flushData = new HashMap<State, List<StateType>>(3);
        for (Map.Entry<StateType, State> stateEntry : states.entrySet()) {
            State state = stateEntry.getValue();
            if (!state.isNeedFlush()) {
                continue;
            }
            StateType stateType = stateEntry.getKey();
            flushData.compute(
                    state,
                    (st, list) -> {
                        if (list == null) {
                            list = new LinkedList<>();
                        }
                        list.add(stateType);
                        return list;
                    });
        }

        for (var entry : flushData.entrySet()) {
            State state = entry.getKey();
            List<StateType> stateTypes = entry.getValue();
            switch (state) {
                case NEEDS_CREATE:
                    createDataFromDatabase(jobKey, stateTypes);
                    break;
                case NEEDS_DELETE:
                    deleteDataFromDatabase(jobKey, stateTypes);
                    break;
                case NEEDS_UPDATE:
                    updateDataFromDatabase(jobKey, stateTypes);
                    break;
                default:
                    throw new IllegalStateException(String.format("Unknown state : %s", state));
            }
            for (var stateType : stateTypes) {
                updateState(stateType, FLUSH_STATE_TRANSITIONER);
            }
        }
    }

    private void updateState(StateType stateType, StateTransition stateTransition) {
        states.compute(
                stateType,
                (type, oldState) -> {
                    checkState(
                            oldState != null,
                            "The state of each state type should be maintained in states. "
                                    + "It may be a bug, please raise a JIRA to Flink Community.");
                    return stateTransition.transition(oldState);
                });
    }

    @VisibleForTesting
    public Map<StateType, String> getDataReadOnly() {
        return Collections.unmodifiableMap(data);
    }

    // TODO extract it to a dataRetriever interface.
    private HashMap<StateType, String> queryFromDatabase(String jobKey) throws SQLException {
        var query =
                "select state_type_id, state_value from t_flink_autoscaler_state_store where job_key = ?";
        var data = new HashMap<StateType, String>();
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            var rs = pstmt.executeQuery();
            while (rs.next()) {
                var stateTypeId = rs.getInt("state_type_id");
                var stateType = StateType.valueOf(stateTypeId);
                var stateValue = rs.getString("state_value");
                data.put(stateType, stateValue);
            }
        }
        return data;
    }

    private void deleteDataFromDatabase(String jobKey, List<StateType> stateTypes)
            throws SQLException {
        var query =
                String.format(
                        "DELETE FROM t_flink_autoscaler_state_store where job_key = ? and state_type_id in (%s)",
                        String.join(",", Collections.nCopies(stateTypes.size(), "?")));
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            int i = 2;
            for (var stateType : stateTypes) {
                pstmt.setInt(i++, stateType.ordinal());
            }
            pstmt.execute();
        }
        LOG.info("Delete jobKey: {} stateTypes: {} from database.", jobKey, stateTypes);
    }

    private void createDataFromDatabase(String jobKey, List<StateType> stateTypes)
            throws SQLException {
        var query =
                "INSERT INTO t_flink_autoscaler_state_store (job_key, state_type_id, state_value) values (?, ?, ?)";
        try (var pstmt = conn.prepareStatement(query)) {
            for (var stateType : stateTypes) {
                pstmt.setString(1, jobKey);
                pstmt.setInt(2, stateType.ordinal());

                String stateValue = data.get(stateType);
                checkState(
                        stateValue != null,
                        "The state value shouldn't be null during inserting. "
                                + "It may be a bug, please raise a JIRA to Flink Community.");
                pstmt.setString(3, stateValue);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
        LOG.info("Insert jobKey: {} stateTypes: {} from database.", jobKey, stateTypes);
    }

    private void updateDataFromDatabase(String jobKey, List<StateType> stateTypes)
            throws SQLException {
        var query =
                "UPDATE t_flink_autoscaler_state_store set state_value = ? where job_key = ? and state_type_id = ?";

        try (var pstmt = conn.prepareStatement(query)) {
            for (var stateType : stateTypes) {
                String stateValue = data.get(stateType);
                checkState(
                        stateValue != null,
                        "The state value shouldn't be null during inserting. "
                                + "It may be a bug, please raise a JIRA to Flink Community.");
                pstmt.setString(1, stateValue);
                pstmt.setString(2, jobKey);
                pstmt.setInt(3, stateType.ordinal());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }
}
