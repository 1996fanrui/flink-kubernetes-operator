package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.autoscaler.JobAutoScalerContext;
import org.apache.flink.autoscaler.event.AutoScalerHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.KubernetesClientUtils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import java.util.HashMap;

import static org.apache.flink.configuration.PipelineOptions.PARALLELISM_OVERRIDES;

/**
 * The kubenetes auto scaler handler.
 *
 * @param <CR>
 */
public class KubernetesAutoScalerHandler<CR extends AbstractFlinkResource<?, ?>>
        implements AutoScalerHandler<ResourceID, CR> {

    private final KubernetesClient kubernetesClient;

    private final EventRecorder eventRecorder;

    public KubernetesAutoScalerHandler(
            KubernetesClient kubernetesClient, EventRecorder eventRecorder) {
        this.kubernetesClient = kubernetesClient;
        this.eventRecorder = eventRecorder;
    }

    @Override
    public void handlerScalingError(
            JobAutoScalerContext<ResourceID, CR> context, String errorMessage) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                EventRecorder.Type.Warning,
                EventRecorder.Reason.AutoscalerError,
                EventRecorder.Component.Operator,
                errorMessage);
    }

    @Override
    public void handlerScalingReport(
            JobAutoScalerContext<ResourceID, CR> context, String scalingReportMessage) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                EventRecorder.Type.Normal,
                EventRecorder.Reason.ScalingReport,
                EventRecorder.Component.Operator,
                scalingReportMessage,
                "ScalingExecutor");
    }

    @Override
    public void handlerIneffectiveScaling(
            JobAutoScalerContext<ResourceID, CR> context, String message) {
        eventRecorder.triggerEvent(
                context.getExtraJobInfo(),
                EventRecorder.Type.Normal,
                EventRecorder.Reason.IneffectiveScaling,
                EventRecorder.Component.Operator,
                message);
    }

    @Override
    public void handlerRecommendedParallelism(
            JobAutoScalerContext<ResourceID, CR> context,
            HashMap<String, String> recommendedParallelism) {
        AbstractFlinkResource<?, ?> resource = context.getExtraJobInfo();
        var flinkConf = Configuration.fromMap(resource.getSpec().getFlinkConfiguration());
        flinkConf.set(PARALLELISM_OVERRIDES, recommendedParallelism);
        resource.getSpec().setFlinkConfiguration(flinkConf.toMap());

        KubernetesClientUtils.applyToStoredCr(
                kubernetesClient,
                resource,
                stored ->
                        stored.getSpec()
                                .setFlinkConfiguration(resource.getSpec().getFlinkConfiguration()));
    }
}
