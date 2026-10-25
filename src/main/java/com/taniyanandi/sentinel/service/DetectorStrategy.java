package com.taniyanandi.sentinel.service;

import com.taniyanandi.sentinel.domain.AnomalyAlert;
import com.taniyanandi.sentinel.domain.DetectionState;
import com.taniyanandi.sentinel.domain.TelemetryEvent;

/**
 * A pluggable anomaly-detection strategy.
 *
 * <p>The active strategy is selected with {@code sentinel.detector.strategy}
 * ({@code zscore} by default, or {@code ewma}). Implementations share the
 * calling convention of the original {@link AnomalyDetector}: the caller
 * (the Kafka Streams topology) owns the {@link DetectionState}, which is
 * kept in the {@code baseline-store} state store per {@code entityId|metric}
 * key, and a {@code null} return means "not anomalous". Keeping that
 * contract means a new strategy needs no topology changes and its state
 * survives restarts exactly like the z-score baseline does.
 */
public interface DetectorStrategy {

    /**
     * Evaluates one event against the per-key baseline state, updating the
     * state with the observed value.
     *
     * @return an {@link AnomalyAlert} if the event is anomalous, otherwise {@code null}
     */
    AnomalyAlert evaluate(TelemetryEvent event, DetectionState state);
}
