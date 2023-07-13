package org.apache.flink.kubernetes.operator.reconciler.deployment;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.JobAutoScalerContext;
import org.apache.flink.kubernetes.operator.autoscaler.event.AutoScalerHandler;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

public class KubernetesAutoScalerHandler implements AutoScalerHandler<ResourceID, AbstractFlinkResource<?, ?>> {

    private final EventRecorder eventRecorder;

    public KubernetesAutoScalerHandler(EventRecorder eventRecorder) {
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void handlerScalingError(JobAutoScalerContext<ResourceID, AbstractFlinkResource<?, ?>> context, String errorMessage) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                EventRecorder.Type.Warning,
                EventRecorder.Reason.AutoscalerError,
                EventRecorder.Component.Operator,
                errorMessage);
    }

    @Override
    public void handlerScalingReport(JobAutoScalerContext<ResourceID, AbstractFlinkResource<?, ?>> context, String scalingReportMessage) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                EventRecorder.Type.Normal,
                EventRecorder.Reason.ScalingReport,
                EventRecorder.Component.Operator,
                scalingReportMessage,
                "ScalingExecutor");
    }

    @Override
    public void handlerIneffectiveScaling(JobAutoScalerContext<ResourceID, AbstractFlinkResource<?, ?>> context, String message) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                EventRecorder.Type.Normal,
                EventRecorder.Reason.IneffectiveScaling,
                EventRecorder.Component.Operator,
                message);
    }
}
