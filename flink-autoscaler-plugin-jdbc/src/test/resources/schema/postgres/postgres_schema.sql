/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- CREATE DATABASE flink_autoscaler;
-- \c flink_autoscaler;

CREATE TABLE t_flink_autoscaler_state_store
(
    id            BIGSERIAL     NOT NULL,
    update_time   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    job_key       TEXT          NOT NULL,
    state_type_id SMALLINT      NOT NULL,
    state_value   TEXT          NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (job_key, state_type_id)
);

-- CREATE OR REPLACE FUNCTION update_update_time_column()
-- RETURNS TRIGGER AS $$
-- BEGIN
--    NEW.update_time = CURRENT_TIMESTAMP;
-- RETURN NEW;
-- END;
-- $$ language 'plpgsql';
--
-- CREATE TRIGGER update_t_flink_autoscaler_state_store_modtime
--     BEFORE UPDATE ON t_flink_autoscaler_state_store
--     FOR EACH ROW
--     EXECUTE FUNCTION update_update_time_column();
