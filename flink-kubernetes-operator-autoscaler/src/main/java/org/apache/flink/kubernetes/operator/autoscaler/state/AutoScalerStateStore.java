package org.apache.flink.kubernetes.operator.autoscaler.state;

import java.util.Optional;

public interface AutoScalerStateStore {

    Optional<String> get(String key);

    // Put the state to state store, please flush the state store to prevent the state lost.
    void put(String key, String value);

    void remove(String key);

    void flush();
}
