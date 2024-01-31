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

package org.apache.flink.autoscaler.jdbc.event;

import org.apache.flink.autoscaler.event.AutoScalerEventHandler;

import java.sql.Connection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/** Responsible for interacting with the database. */
public class JdbcEventInteractor {

    private final Connection conn;

    public JdbcEventInteractor(Connection conn) {
        this.conn = conn;
    }

    public Optional<AutoScalerEvent> queryLatestEvent(String jobKey, String reason, String eventKey)
            throws Exception {
        var query =
                "select * from t_flink_autoscaler_event_handler "
                        + "where job_key = ? and reason = ? and event_key = ? "
                        + "order by create_time desc limit 1";

        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            pstmt.setString(2, reason);
            pstmt.setString(3, eventKey);

            var rs = pstmt.executeQuery();
            AutoScalerEvent event = null;
            if (rs.next()) {
                event =
                        new AutoScalerEvent(
                                rs.getLong("id"),
                                rs.getTimestamp("create_time").toInstant(),
                                rs.getTimestamp("update_time").toInstant(),
                                rs.getString("job_key"),
                                rs.getString("reason"),
                                rs.getString("event_type"),
                                rs.getString("message"),
                                rs.getInt("count"),
                                rs.getString("event_key"));
            }
            return Optional.ofNullable(event);
        }
    }

    public void createEvent(
            String jobKey,
            String reason,
            AutoScalerEventHandler.Type type,
            String message,
            String eventKey)
            throws Exception {
        var query =
                "INSERT INTO t_flink_autoscaler_event_handler (job_key, reason, event_type, message, `count`, event_key)"
                        + " values (?, ?, ?, ?, ?, ?)";
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, jobKey);
            pstmt.setString(2, reason);
            pstmt.setString(3, type.toString());
            pstmt.setString(4, message);
            pstmt.setInt(5, 1);
            pstmt.setString(6, eventKey);
            pstmt.executeUpdate();
        }
    }

    public void updateEvent(long id, String message, int count) throws Exception {
        var query =
                "UPDATE t_flink_autoscaler_event_handler set message = ?, `count` = ? where id = ?";

        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, message);
            pstmt.setInt(2, count);
            pstmt.setLong(3, id);
            checkState(pstmt.executeUpdate() == 1, "Update event id=[%s] fails.", id);
        }
    }
}
