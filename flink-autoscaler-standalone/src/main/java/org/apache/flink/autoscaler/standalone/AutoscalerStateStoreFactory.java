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

package org.apache.flink.autoscaler.standalone;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.jdbc.state.JDBCAutoScalerStateStore;
import org.apache.flink.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.autoscaler.state.InMemoryAutoScalerStateStore;

import java.sql.DriverManager;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The factory of {@link AutoScalerStateStore}. */
public class AutoscalerStateStoreFactory {

    private static final String STATE_STORE_TYPE = "state-store.type";
    private static final String IN_MEMORY_STATE_STORE = "memory";

    private static final String JDBC_STATE_STORE = "jdbc";
    private static final String JDBC_STATE_STORE_URL = "state-store.jdbc.url";
    private static final String JDBC_STATE_STORE_USER = "state-store.jdbc.username";
    private static final String JDBC_STATE_STORE_PASSWORD = "state-store.jdbc.password";

    public static <KEY, Context extends JobAutoScalerContext<KEY>>
            AutoScalerStateStore<KEY, Context> create(ParameterTool parameters) throws Exception {
        var stateStoreType = parameters.get(STATE_STORE_TYPE, IN_MEMORY_STATE_STORE).toLowerCase();
        switch (stateStoreType) {
            case IN_MEMORY_STATE_STORE:
                return new InMemoryAutoScalerStateStore<>();
            case JDBC_STATE_STORE:
                return createJDBCStateStore(parameters);
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown state store type : %s. Optional state store types are: %s and %s.",
                                stateStoreType, IN_MEMORY_STATE_STORE, JDBC_STATE_STORE));
        }
    }

    private static <KEY, Context extends JobAutoScalerContext<KEY>>
            AutoScalerStateStore<KEY, Context> createJDBCStateStore(ParameterTool parameters)
                    throws Exception {
        var jdbcUrl = parameters.get(JDBC_STATE_STORE_URL);
        checkNotNull(jdbcUrl, "%s is required for jdbc state store.", JDBC_STATE_STORE_URL);
        var user = parameters.get(JDBC_STATE_STORE_USER);
        var password = parameters.get(JDBC_STATE_STORE_PASSWORD);

        var conn = DriverManager.getConnection(jdbcUrl, user, password);
        return new JDBCAutoScalerStateStore<>(conn);
    }
}
