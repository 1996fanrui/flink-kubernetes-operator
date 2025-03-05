package org.apache.flink.autoscaler.error.checker;

import org.apache.flink.api.common.JobID;
import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsHeaders;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfoWithHistory;
import org.apache.flink.runtime.rest.messages.job.JobExceptionsMessageParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Check whether network memory is insufficient. */
public class NetworkMemoryInsufficientChecker<KEY, Context extends JobAutoScalerContext<KEY>>
        implements JobUnrecoverableErrorChecker<KEY, Context> {

    private static final Logger LOG =
            LoggerFactory.getLogger(NetworkMemoryInsufficientChecker.class);

    @Override
    public boolean check(Context context) throws Exception {
        final JobID jobID = context.getJobID();
        if (jobID == null) {
            return false;
        }
        var messageParameters = new JobExceptionsMessageParameters();
        messageParameters.jobPathParameter.resolve(jobID);

        JobExceptionsInfoWithHistory aa =
                context.getRestClusterClient()
                        .sendRequest(
                                JobExceptionsHeaders.getInstance(),
                                messageParameters,
                                EmptyRequestBody.getInstance())
                        .get();

        for (var exceptionEntry : aa.getExceptionHistory().getEntries()) {
            if (exceptionEntry.getStacktrace().contains("Insufficient number of network buffers")) {
                LOG.info(
                        "Found Insufficient number of network buffers exception, it's unrecoverable error if job isn't changed.");
                return true;
            }
        }

        return false;
    }
}
