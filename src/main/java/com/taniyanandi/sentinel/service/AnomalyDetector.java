package com.taniyanandi.sentinel.service;

import com.taniyanandi.sentinel.domain.AnomalyAlert;
import com.taniyanandi.sentinel.domain.DetectionState;
import com.taniyanandi.sentinel.domain.TelemetryEvent;
import com.taniyanandi.sentinel.metrics.SentinelMetrics;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

/**
 * Z-score detector: the default {@link DetectorStrategy}. Active unless
 * {@code sentinel.detector.strategy} selects another strategy (for example
 * {@code ewma}).
 */
@Service
@ConditionalOnProperty(name = "sentinel.detector.strategy", havingValue = "zscore", matchIfMissing = true)
public class AnomalyDetector implements DetectorStrategy {
    private final long warmupEvents; private final double zThreshold; private final SentinelMetrics metrics;
    public AnomalyDetector(@Value("${sentinel.detector.warmup-events:30}") long warmupEvents,
                           @Value("${sentinel.detector.z-threshold:3.0}") double zThreshold, SentinelMetrics metrics) {
        this.warmupEvents = warmupEvents; this.zThreshold = zThreshold; this.metrics = metrics;
    }
    @Override
    public AnomalyAlert evaluate(TelemetryEvent event, DetectionState state) {
        return metrics.detectionLatency().record(() -> evaluateInternal(event, state));
    }
    private AnomalyAlert evaluateInternal(TelemetryEvent event, DetectionState state) {
        if (state.count() < warmupEvents || state.stdDev() < 1e-9) { state.observe(event.value()); return null; }
        double mean = state.mean(), stdDev = state.stdDev();
        double z = Math.abs(event.value() - mean) / stdDev; state.observe(event.value());
        if (z < zThreshold) return null;
        AnomalyAlert.Severity severity = z >= 6 ? AnomalyAlert.Severity.CRITICAL : z >= 5 ? AnomalyAlert.Severity.HIGH : z >= 4 ? AnomalyAlert.Severity.MEDIUM : AnomalyAlert.Severity.LOW;
        metrics.anomalyDetected(severity == AnomalyAlert.Severity.CRITICAL);
        return new AnomalyAlert(UUID.randomUUID().toString(), event.eventId(), event.entityId(), event.metric(), event.value(), mean, stdDev, z, severity,
                "Value deviated " + String.format("%.2f", z) + " standard deviations from baseline", Instant.now());
    }
}
