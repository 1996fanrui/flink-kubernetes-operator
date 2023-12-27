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

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Countable {@link JDBCInteractor}. */
public class CountableJDBCInteractor extends JDBCInteractor {

    private final AtomicLong queryCounter;
    private final AtomicLong deleteCounter;
    private final AtomicLong createCounter;
    private final AtomicLong updateCounter;

    public CountableJDBCInteractor(Connection conn) {
        super(conn);
        queryCounter = new AtomicLong();
        deleteCounter = new AtomicLong();
        createCounter = new AtomicLong();
        updateCounter = new AtomicLong();
    }

    @Override
    public HashMap<StateType, String> queryData(String jobKey) throws Exception {
        queryCounter.incrementAndGet();
        return super.queryData(jobKey);
    }

    @Override
    public void deleteData(String jobKey, List<StateType> deletedStateTypes) throws Exception {
        deleteCounter.incrementAndGet();
        super.deleteData(jobKey, deletedStateTypes);
    }

    @Override
    public void createData(
            String jobKey, List<StateType> createdStateTypes, HashMap<StateType, String> data)
            throws Exception {
        createCounter.incrementAndGet();
        super.createData(jobKey, createdStateTypes, data);
    }

    @Override
    public void updateData(
            String jobKey, List<StateType> updatedStateTypes, HashMap<StateType, String> data)
            throws Exception {
        updateCounter.incrementAndGet();
        super.updateData(jobKey, updatedStateTypes, data);
    }

    public long getQueryCounter() {
        return queryCounter.get();
    }

    public long getDeleteCounter() {
        return deleteCounter.get();
    }

    public long getCreateCounter() {
        return createCounter.get();
    }

    public long getUpdateCounter() {
        return updateCounter.get();
    }
}
