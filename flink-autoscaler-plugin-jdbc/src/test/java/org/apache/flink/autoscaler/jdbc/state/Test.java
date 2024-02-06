package org.apache.flink.autoscaler.jdbc.state;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.apache.flink.autoscaler.jdbc.state.StateType.COLLECTED_METRICS;
import static org.apache.flink.autoscaler.jdbc.state.StateType.SCALING_HISTORY;
import static org.assertj.core.api.Assertions.assertThat;

public class Test {

    @org.junit.jupiter.api.Test
    void test() throws Exception {
        final var conn =
                DriverManager.getConnection(
                        //                        "jdbc:mysql://localhost:3306/flink_autoscaler",
                        // "root", "123456");
                        "jdbc:postgresql://localhost:5432/flink_autoscaler", "root", "123456");
        final JdbcStateInteractor jdbcStateInteractor = new JdbcStateInteractor(conn);

        var jobKey = "jobKey1";
        var value1 = "value1";
        var value2 = "value2";
        var value3 = "value3";
        jdbcStateInteractor.createData(
                jobKey,
                List.of(COLLECTED_METRICS, SCALING_HISTORY),
                Map.of(COLLECTED_METRICS, value1, SCALING_HISTORY, value2));

        Thread.sleep(10000);

        jdbcStateInteractor.updateData(
                jobKey,
                List.of(COLLECTED_METRICS),
                Map.of(COLLECTED_METRICS, value3, SCALING_HISTORY, value2));
        assertThat(jdbcStateInteractor.queryData(jobKey))
                .isEqualTo(Map.of(COLLECTED_METRICS, value3, SCALING_HISTORY, value2));

        conn.close();
    }
}
