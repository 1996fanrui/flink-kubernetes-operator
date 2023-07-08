package org.apache.flink.kubernetes.operator.autoscaler.event;

import org.apache.flink.kubernetes.operator.autoscaler.JobAutoScalerContext;

import javax.annotation.Nullable;
import java.util.HashMap;

public interface AutoScalerHandler<KEY, INFO> {

    void handlerScalingError(JobAutoScalerContext<KEY, INFO> context, String errorMessage);

    void handlerScalingReport(JobAutoScalerContext<KEY, INFO> context, String scalingReportMessage);

    void handlerIneffectiveScaling(JobAutoScalerContext<KEY, INFO> context, String message);

    void handlerRecommendedParallelism(JobAutoScalerContext<KEY, INFO> context, HashMap<String, String> recommendedParallelism);
}
