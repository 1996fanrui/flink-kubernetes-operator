package org.apache.flink.autoscaler.error.checker;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;

/** Check whether network memory is insufficient. */
public class NetworkMemoryInsufficientChecker<KEY, Context extends JobAutoScalerContext<KEY>>
        implements JobUnrecoverableErrorChecker<KEY, Context> {

    @Override
    public boolean check(Context context, EvaluatedMetrics evaluatedMetrics) throws Exception {
        context.getRestClusterClient()
                .sendRequest(
                        JobExceptionsHeaders.getInstance(),
                        new JobExceptionsMessageParameters(),
                        EmptyRequestBody.getInstance());
        return false;
    }
}
