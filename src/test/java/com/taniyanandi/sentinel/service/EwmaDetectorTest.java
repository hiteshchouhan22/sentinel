package com.taniyanandi.sentinel.service;

import com.taniyanandi.sentinel.domain.AnomalyAlert;
import com.taniyanandi.sentinel.domain.DetectionState;
import com.taniyanandi.sentinel.metrics.SentinelMetrics;
import com.taniyanandi.sentinel.domain.TelemetryEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class EwmaDetectorTest {

    private EwmaDetector detector() {
        SentinelMetrics metrics = new SentinelMetrics(new SimpleMeterRegistry());
        return new EwmaDetector(5, 0.3, 3.0, metrics);
    }

    /** Feeds a stable baseline of ~10 into the state, past the warm-up of 5. */
    private DetectionState warmedUpState(EwmaDetector detector) {
        DetectionState state = new DetectionState();
        for (double value : new double[]{10, 11, 10, 11, 10}) {
            assertThat(detector.evaluate(event(value), state)).isNull();
        }
        return state;
    }

    @Test
    void emitsCriticalAlertForLargeDeviation() {
        EwmaDetector detector = detector();
        DetectionState state = warmedUpState(detector);

        AnomalyAlert alert = detector.evaluate(event(50), state);

        assertThat(alert).isNotNull();
        assertThat(alert.severity()).isEqualTo(AnomalyAlert.Severity.CRITICAL);
        assertThat(alert.zScore()).isGreaterThan(3.0);
        assertThat(alert.baselineMean()).isBetween(10.0, 11.0);
        assertThat(alert.reason()).contains("EWMA");
    }

    @Test
    void noAlertForValueWithinBaseline() {
        EwmaDetector detector = detector();
        DetectionState state = warmedUpState(detector);

        assertThat(detector.evaluate(event(10.5), state)).isNull();
    }

    @Test
    void noAlertDuringWarmupEvenForExtremeValue() {
        EwmaDetector detector = detector();
        DetectionState state = new DetectionState();

        assertThat(detector.evaluate(event(1000), state)).isNull();
        assertThat(state.ewmaCount()).isEqualTo(1);
        assertThat(state.ewmaMean()).isEqualTo(1000.0);
    }

    @Test
    void adaptsToSustainedLevelShift() {
        EwmaDetector detector = detector();
        DetectionState state = warmedUpState(detector);

        AnomalyAlert last = null;
        for (int i = 0; i < 20; i++) {
            last = detector.evaluate(event(20), state);
        }

        // After the shift, 20 is the new normal: the EWMA baseline has moved
        // to it, unlike an all-history z-score baseline.
        assertThat(last).isNull();
        assertThat(state.ewmaMean()).isCloseTo(20.0, org.assertj.core.api.Assertions.within(0.5));
    }

    @Test
    void rejectsInvalidAlpha() {
        SentinelMetrics metrics = new SentinelMetrics(new SimpleMeterRegistry());
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EwmaDetector(5, 0.0, 3.0, metrics));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EwmaDetector(5, 1.5, 3.0, metrics));
    }

    private TelemetryEvent event(double value) {
        return new TelemetryEvent("e-" + value, "test", "entity-1", "latency_ms", value, Instant.now(), Map.of());
    }
}
