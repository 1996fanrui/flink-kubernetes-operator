package org.apache.flink.kubernetes.operator.reconciler.deployment;

import org.apache.flink.kubernetes.operator.api.AbstractFlinkResource;
import org.apache.flink.kubernetes.operator.autoscaler.state.AutoScalerStateStore;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** The kubernetes auto scaler state store. */
public class KubernetesAutoScalerStateStore implements AutoScalerStateStore {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesAutoScalerStateStore.class);

    private static final String LABEL_COMPONENT_AUTOSCALER = "autoscaler";

    private final KubernetesClient kubernetesClient;

    private final ConfigMap configMap;

    public KubernetesAutoScalerStateStore(
            AbstractFlinkResource<?, ?> cr, KubernetesClient kubernetesClient) {
        this.kubernetesClient = kubernetesClient;
        this.configMap = getConfigMap(cr, kubernetesClient);
    }

    public static ConfigMap getConfigMap(
            AbstractFlinkResource<?, ?> cr, KubernetesClient kubeClient) {

        var objectMeta = new ObjectMeta();
        objectMeta.setName("autoscaler-" + cr.getMetadata().getName());
        objectMeta.setNamespace(cr.getMetadata().getNamespace());

        return getScalingInfoConfigMapFromKube(objectMeta, kubeClient)
                .orElseGet(
                        () -> {
                            LOG.info("Creating scaling info config map");

                            objectMeta.setLabels(
                                    Map.of(
                                            Constants.LABEL_COMPONENT_KEY,
                                            LABEL_COMPONENT_AUTOSCALER,
                                            Constants.LABEL_APP_KEY,
                                            cr.getMetadata().getName()));
                            var cm = new ConfigMap();
                            cm.setMetadata(objectMeta);
                            cm.addOwnerReference(cr);
                            cm.setData(new HashMap<>());
                            return kubeClient.resource(cm).create();
                        });
    }

    private static Optional<ConfigMap> getScalingInfoConfigMapFromKube(
            ObjectMeta objectMeta, KubernetesClient kubeClient) {
        return Optional.ofNullable(
                kubeClient
                        .configMaps()
                        .inNamespace(objectMeta.getNamespace())
                        .withName(objectMeta.getName())
                        .get());
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(configMap.getData().get(key));
    }

    @Override
    public void put(String key, String value) {
        configMap.getData().put(key, value);
    }

    @Override
    public void remove(String key) {
        configMap.getData().remove(key);
    }

    @Override
    public void flush() {
        kubernetesClient.resource(configMap).update();
    }
}
