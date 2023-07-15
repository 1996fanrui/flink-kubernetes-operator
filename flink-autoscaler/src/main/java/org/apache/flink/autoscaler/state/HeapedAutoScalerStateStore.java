package org.apache.flink.autoscaler.state;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Optional;

/** It's thread-unsafe. */
@NotThreadSafe
public class HeapedAutoScalerStateStore implements AutoScalerStateStore {

    private final HashMap<String, String> state;

    public HeapedAutoScalerStateStore() {
        state = new HashMap<>();
    }

    @Override
    public Optional<String> get(String key) {
        return Optional.ofNullable(state.get(key));
    }

    @Override
    public void put(String key, String value) {
        state.put(key, value);
    }

    @Override
    public void remove(String key) {
        state.remove(key);
    }

    @Override
    public void flush() {
        // Nothing to do
    }
}
