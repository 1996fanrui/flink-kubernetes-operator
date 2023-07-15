package org.apache.flink.autoscaler.event;

import org.apache.flink.autoscaler.JobAutoScalerContext;

import java.util.HashMap;

/**
 * Handler all events during scaling.
 *
 * @param <KEY>
 * @param <INFO>
 */
public interface AutoScalerHandler<KEY, INFO> {

    void handlerScalingError(JobAutoScalerContext<KEY, INFO> context, String errorMessage);

    void handlerScalingReport(JobAutoScalerContext<KEY, INFO> context, String scalingReportMessage);

    void handlerIneffectiveScaling(JobAutoScalerContext<KEY, INFO> context, String message);

    void handlerRecommendedParallelism(
            JobAutoScalerContext<KEY, INFO> context,
            HashMap<String, String> recommendedParallelism);
}
