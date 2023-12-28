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

package org.apache.flink.autoscaler.jdbc.testutils.databases.derby;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/** The extension of Derby. */
public class DerbyExtension implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {

    private static final List<String> TABLES = List.of("t_flink_autoscaler_state_store");
    private static final String JDBC_URL = "jdbc:derby:memory:test";

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection(JDBC_URL);
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        DriverManager.getConnection(String.format("%s;create=true", JDBC_URL)).close();

        var stateDDL =
                "create table t_flink_autoscaler_state_store\n"
                        + "(\n"
                        + "    id            bigint,\n"
                        + "    job_key       varchar(191) not null ,\n"
                        + "    state_type_id int      not null ,\n"
                        + "    state_value   varchar(1000)     not null \n"
                        + ")";

        try (var statement = getConnection().createStatement()) {
            statement.execute(stateDDL);
        }
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        try {
            DriverManager.getConnection(String.format("%s;shutdown=true", JDBC_URL)).close();
        } catch (SQLException ignored) {
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        Connection conn = getConnection();
        // Clean up all data
        for (var tableName : TABLES) {
            try (var st = conn.createStatement()) {
                st.executeUpdate(String.format("DELETE from %s", tableName));
            }
        }
    }
}
