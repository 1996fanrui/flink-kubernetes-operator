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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Responsible for interacting with the database. */
public class JDBCStateInteractor {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCStateInteractor.class);

    private final Connection conn;

    public JDBCStateInteractor(Connection conn) {
        this.conn = conn;
    }

    public Map<StateType, String> queryData(String jobKey) throws Exception {
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

    public void deleteData(String jobKey, List<StateType> deletedStateTypes) throws Exception {
        var query =
                String.format(
                        "DELETE FROM t_flink_autoscaler_state_store where job_key = ? and state_type_id in (%s)",
                        String.join(",", Collections.nCopies(deletedStateTypes.size(), "?")));
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            int i = 2;
            for (var stateType : deletedStateTypes) {
                pstmt.setInt(i++, stateType.ordinal());
            }
            pstmt.execute();
        }
        LOG.info("Delete jobKey: {} stateTypes: {} from database.", jobKey, deletedStateTypes);
    }

    public void createData(
            String jobKey, List<StateType> createdStateTypes, Map<StateType, String> data)
            throws Exception {
        var query =
                "INSERT INTO t_flink_autoscaler_state_store (job_key, state_type_id, state_value) values (?, ?, ?)";
        try (var pstmt = conn.prepareStatement(query)) {
            for (var stateType : createdStateTypes) {
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
        LOG.info("Insert jobKey: {} stateTypes: {} from database.", jobKey, createdStateTypes);
    }

    public void updateData(
            String jobKey, List<StateType> updatedStateTypes, Map<StateType, String> data)
            throws Exception {
        var query =
                "UPDATE t_flink_autoscaler_state_store set state_value = ? where job_key = ? and state_type_id = ?";

        try (var pstmt = conn.prepareStatement(query)) {
            for (var stateType : updatedStateTypes) {
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
