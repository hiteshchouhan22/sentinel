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
 * Exponentially Weighted Moving Average (EWMA) detector, an alternative to
 * the z-score {@link AnomalyDetector}.
 *
 * <p>The z-score baseline weights every historical observation equally, so
 * it reacts slowly when a metric's normal level drifts. EWMA weights recent
 * observations more heavily (controlled by {@code alpha}), so the baseline
 * tracks trend shifts while still flagging sudden deviations: an event is
 * anomalous when {@code |value - ewmaMean| > threshold * ewmaStdDev}, the
 * same threshold semantics as the z-score strategy.
 *
 * <p>Active only when {@code sentinel.detector.strategy=ewma} is set; the
 * default remains {@code zscore}, so existing deployments are unaffected.
 */
@Service
@ConditionalOnProperty(name = "sentinel.detector.strategy", havingValue = "ewma")
public class EwmaDetector implements DetectorStrategy {
    private final long warmupEvents;
    private final double alpha;
    private final double threshold;
    private final SentinelMetrics metrics;

    public EwmaDetector(@Value("${sentinel.detector.warmup-events:30}") long warmupEvents,
                        @Value("${sentinel.detector.ewma-alpha:0.3}") double alpha,
                        @Value("${sentinel.detector.z-threshold:3.0}") double threshold,
                        SentinelMetrics metrics) {
        if (alpha <= 0.0 || alpha > 1.0) {
            throw new IllegalArgumentException(
                    "sentinel.detector.ewma-alpha must be in (0, 1], got " + alpha);
        }
        this.warmupEvents = warmupEvents;
        this.alpha = alpha;
        this.threshold = threshold;
        this.metrics = metrics;
    }

    @Override
    public AnomalyAlert evaluate(TelemetryEvent event, DetectionState state) {
        return metrics.detectionLatency().record(() -> evaluateInternal(event, state));
    }

    private AnomalyAlert evaluateInternal(TelemetryEvent event, DetectionState state) {
        if (state.ewmaCount() < warmupEvents || state.ewmaStdDev() < 1e-9) {
            state.observeEwma(event.value(), alpha);
            return null;
        }
        double mean = state.ewmaMean(), stdDev = state.ewmaStdDev();
        double score = Math.abs(event.value() - mean) / stdDev;
        state.observeEwma(event.value(), alpha);
        if (score < threshold) return null;
        AnomalyAlert.Severity severity = score >= 6 ? AnomalyAlert.Severity.CRITICAL
                : score >= 5 ? AnomalyAlert.Severity.HIGH
                : score >= 4 ? AnomalyAlert.Severity.MEDIUM
                : AnomalyAlert.Severity.LOW;
        metrics.anomalyDetected(severity == AnomalyAlert.Severity.CRITICAL);
        return new AnomalyAlert(UUID.randomUUID().toString(), event.eventId(), event.entityId(),
                event.metric(), event.value(), mean, stdDev, score, severity,
                "Value deviated " + String.format("%.2f", score)
                        + " exponentially weighted standard deviations from the EWMA baseline",
                Instant.now());
    }
}
