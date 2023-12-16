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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

/** The extension of MySQL 5.7. */
public class MySQL57Extension implements BeforeAllCallback, AfterAllCallback, AfterEachCallback {

    private static final String MYSQL_INIT_SCRIPT = "mysql_ddl.sql";
    private static final String DATABASE_NAME = "flink_autoscaler";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "123456";
    private static final List<String> TABLES = List.of("t_flink_autoscaler_state_store");

    private static final MySQLContainer<?> CONTAINER =
            new MySQLContainer<>("mysql:5.7.41")
                    .withCommand("--character-set-server=utf8")
                    .withDatabaseName(DATABASE_NAME)
                    .withUsername(USER_NAME)
                    .withPassword(PASSWORD)
                    .withInitScript(MYSQL_INIT_SCRIPT)
                    .withEnv("MYSQL_ROOT_HOST", "%");

    public static Connection getConnection() throws Exception {
        return DriverManager.getConnection(
                String.format("%s/%s", CONTAINER.getJdbcUrl(), CONTAINER.getDatabaseName()),
                CONTAINER.getUsername(),
                CONTAINER.getPassword());
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        CONTAINER.start();
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        CONTAINER.stop();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        Connection conn = getConnection();
        for (var tableName : TABLES) {
            try (Statement st = conn.createStatement()) {
                st.executeUpdate(String.format("DELETE from %s.%s", DATABASE_NAME, tableName));
            }
        }
    }
}
