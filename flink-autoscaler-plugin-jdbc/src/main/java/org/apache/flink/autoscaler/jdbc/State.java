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

import javax.annotation.Nonnull;

import static org.apache.flink.autoscaler.jdbc.State.NEEDS_CREATE;
import static org.apache.flink.autoscaler.jdbc.State.NEEDS_DELETE;
import static org.apache.flink.autoscaler.jdbc.State.NEEDS_UPDATE;
import static org.apache.flink.autoscaler.jdbc.State.NOT_NEEDED;
import static org.apache.flink.autoscaler.jdbc.State.UP_TO_DATE;

/**
 * The state of state type about the cache and database.
 *
 * <p>Note: {@link #inLocally} and {@link #inDatabase} are only for understand, we don't use them.
 */
enum State {

    /** State doesn't exist at database, and it's not used so far, so it's not needed. */
    NOT_NEEDED(false, false, false),
    /** State is only stored locally, not created in JDBC database yet. */
    NEEDS_CREATE(true, false, true),
    /** State exists in JDBC database but there are newer local changes. */
    NEEDS_UPDATE(true, true, true),
    /** State is stored locally and in database, and they are same. */
    UP_TO_DATE(true, true, false),
    /** State is stored in database, but it's deleted in local. */
    NEEDS_DELETE(false, true, true);

    /** The data of this state type is stored in locally when it is true. */
    private final boolean inLocally;

    /** The data of this state type is stored in database when it is true. */
    private final boolean inDatabase;

    /** The data of this state type is stored in database when it is true. */
    private final boolean needFlush;

    State(boolean inLocally, boolean inDatabase, boolean needFlush) {
        this.inLocally = inLocally;
        this.inDatabase = inDatabase;
        this.needFlush = needFlush;
    }

    public boolean isNeedFlush() {
        return needFlush;
    }
}

/** Transition old state to the new state when some actions happen. */
interface StateTransition {

    @Nonnull
    State transition(@Nonnull State oldState);
}

/** All states will be {@link State#NEEDS_CREATE} or {@link State#NEEDS_UPDATE} after putting. */
class PutStateTransitioner implements StateTransition {

    @Nonnull
    @Override
    public State transition(@Nonnull State oldState) {
        switch (oldState) {
            case NOT_NEEDED:
            case NEEDS_CREATE:
                return NEEDS_CREATE;
            case NEEDS_UPDATE:
            case UP_TO_DATE:
            case NEEDS_DELETE:
                return NEEDS_UPDATE;
            default:
                throw new IllegalArgumentException(String.format("Unknown state : %s.", oldState));
        }
    }
}

/** All states will be {@link State#NOT_NEEDED} or {@link State#NEEDS_DELETE} after deleting. */
class DeleteStateTransitioner implements StateTransition {

    @Nonnull
    @Override
    public State transition(@Nonnull State oldState) {
        switch (oldState) {
            case NOT_NEEDED:
            case NEEDS_CREATE:
                return NOT_NEEDED;
            case NEEDS_UPDATE:
            case UP_TO_DATE:
            case NEEDS_DELETE:
                return NEEDS_DELETE;
            default:
                throw new IllegalArgumentException(String.format("Unknown state : %s.", oldState));
        }
    }
}

/** All states will be {@link State#NOT_NEEDED} or {@link State#UP_TO_DATE} after flushing. */
class FlushStateTransitioner implements StateTransition {

    @Nonnull
    @Override
    public State transition(@Nonnull State oldState) {
        switch (oldState) {
            case NOT_NEEDED:
            case NEEDS_DELETE:
                return NOT_NEEDED;
            case NEEDS_CREATE:
            case NEEDS_UPDATE:
            case UP_TO_DATE:
                return UP_TO_DATE;
            default:
                throw new IllegalArgumentException(String.format("Unknown state : %s.", oldState));
        }
    }
}
