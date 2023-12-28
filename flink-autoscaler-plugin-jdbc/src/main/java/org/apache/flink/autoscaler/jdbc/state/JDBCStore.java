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

import org.apache.flink.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** The jdbc store. */
public class JDBCStore {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCStore.class);

    private final ConcurrentHashMap<String, JobStateView> cache = new ConcurrentHashMap<>();

    private final JDBCStateInteractor jdbcStateInteractor;

    public JDBCStore(JDBCStateInteractor jdbcStateInteractor) throws SQLException {
        this.jdbcStateInteractor = jdbcStateInteractor;

        //        String url1 = "jdbc:mysql://localhost:3306/mydatabase";
        // TODO
        //        conn.setAutoCommit(false);
    }

    protected void putSerializedState(String jobKey, StateType stateType, String value) {
        getJobStateView(jobKey).put(stateType, value);
    }

    protected Optional<String> getSerializedState(String jobKey, StateType stateType) {
        return Optional.ofNullable(getJobStateView(jobKey).get(stateType));
    }

    protected void removeSerializedState(String jobKey, StateType stateType) {
        getJobStateView(jobKey).remove(stateType);
    }

    public void flush(String jobKey) throws Exception {
        JobStateView jobStateView = cache.get(jobKey);
        if (jobStateView == null) {
            LOG.debug("The JobStateView doesn't exist, so skip the flush.");
            return;
        }
        try {
            jobStateView.flush();
        } catch (Exception e) {
            LOG.error(
                    "Error while flush autoscaler info to database, invalidating to clear the cache",
                    e);
            removeInfoFromCache(jobKey);
            throw e;
        }
    }

    public void removeInfoFromCache(String jobKey) {
        cache.remove(jobKey);
    }

    public void clearAll(String jobKey) {
        getJobStateView(jobKey).clear();
    }

    @VisibleForTesting
    JobStateView getJobStateView(String jobKey) {
        return cache.computeIfAbsent(
                jobKey,
                (id) -> {
                    try {
                        return createJobStateView(jobKey);
                    } catch (Exception exception) {
                        // TODO test it.
                        throw new RuntimeException(
                                "Meet exception during create job state view.", exception);
                    }
                });
    }

    private JobStateView createJobStateView(String jobKey) throws Exception {
        return new JobStateView(jdbcStateInteractor, jobKey);
    }
}
