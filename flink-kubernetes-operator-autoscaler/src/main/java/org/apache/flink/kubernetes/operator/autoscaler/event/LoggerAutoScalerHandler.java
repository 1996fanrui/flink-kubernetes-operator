package org.apache.flink.kubernetes.operator.autoscaler.event;

import org.apache.flink.kubernetes.operator.autoscaler.JobAutoScalerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * The logged auto scaler handler.
 *
 * @param <KEY>
 * @param <INFO>
 */
public class LoggerAutoScalerHandler<KEY, INFO> implements AutoScalerHandler<KEY, INFO> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggerAutoScalerHandler.class);

    @Override
    public void handlerScalingError(JobAutoScalerContext<KEY, INFO> context, String errorMessage) {

        //        String logMessage = String.format("Auto scaler event of jobId=[%s], reason is %s,
        // messageKey is %s, message is %s.",
        //                context.getJobID(), reason, messageKey, message);
        //        if (isWarning) {
        //            LOG.warn(logMessage);
        //            return;
        //        }
        //        LOG.info(logMessage);
    }

    @Override
    public void handlerScalingReport(
            JobAutoScalerContext<KEY, INFO> context, String scalingReportMessage) {}

    @Override
    public void handlerIneffectiveScaling(
            JobAutoScalerContext<KEY, INFO> context, String message) {}

    @Override
    public void handlerRecommendedParallelism(
            JobAutoScalerContext<KEY, INFO> context,
            HashMap<String, String> recommendedParallelism) {}
}
