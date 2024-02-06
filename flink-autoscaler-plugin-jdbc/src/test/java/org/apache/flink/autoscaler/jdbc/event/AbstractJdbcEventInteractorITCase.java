package org.apache.flink.autoscaler.jdbc.event;

import org.apache.flink.autoscaler.event.AutoScalerEventHandler;
import org.apache.flink.autoscaler.jdbc.testutils.databases.DatabaseTest;
import org.apache.flink.autoscaler.jdbc.testutils.databases.derby.DerbyTestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL56TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL57TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.mysql.MySQL8TestBase;
import org.apache.flink.autoscaler.jdbc.testutils.databases.postgres.PostgreSQLTestBase;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** The abstract IT case for {@link JdbcEventInteractor}. */
abstract class AbstractJdbcEventInteractorITCase implements DatabaseTest {


    @Test
    void testAllOperations() throws Exception {
        var jobKey = "jobKey";
        var reason = "ScalingReport";
        var message = "Expected event message.";
        var eventKey = Integer.toString(34567);

        try (var conn = getConnection()) {
            var jdbcEventInteractor = new JdbcEventInteractor(conn);
            jdbcEventInteractor.createEvent(jobKey, reason, AutoScalerEventHandler.Type.Normal, message, eventKey);
            var eventOptional = jdbcEventInteractor.queryLatestEvent(jobKey, reason, eventKey);
            assertThat(eventOptional).isPresent();
            assertEvent(eventOptional.get(), jobKey, reason, message, eventKey);

            // Sleep to ensure the create time are not same.
            Thread.sleep(3);
            jdbcEventInteractor.createEvent(jobKey, reason, AutoScalerEventHandler.Type.Normal, message, eventKey);



        }
    }

    private void assertEvent(AutoScalerEvent event, String jobKey, String reason, String message, String eventKey) {
        assertThat(event.getCreateTime()).isEqualTo(event.getUpdateTime());
        assertThat(event.getJobKey()).isEqualTo(jobKey);
        assertThat(event.getReason()).isEqualTo(reason);
        assertThat(event.getEventType()).isEqualTo(AutoScalerEventHandler.Type.Normal.toString());
        assertThat(event.getMessage()).isEqualTo(message);
        assertThat(event.getCount()).isOne();
        assertThat(event.getEventKey()).isEqualTo(eventKey);
    }

}

/** Test {@link JdbcEventInteractor} via Derby database. */
class DerbyJdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements DerbyTestBase {}

/** Test {@link JdbcEventInteractor} via MySQL 5.6.x. */
class MySQL56JdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements MySQL56TestBase {}

/** Test {@link JdbcEventInteractor} via MySQL 5.7.x. */
class MySQL57JdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements MySQL57TestBase {}

/** Test {@link JdbcEventInteractor} via MySQL 8.x. */
class MySQL8JdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements MySQL8TestBase {}

/** Test {@link JdbcEventInteractor} via Postgre SQL. */
class PostgreSQLJdbcEventInteractorITCase extends AbstractJdbcEventInteractorITCase
        implements PostgreSQLTestBase {}
