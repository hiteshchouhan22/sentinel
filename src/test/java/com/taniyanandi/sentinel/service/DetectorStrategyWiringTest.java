package com.taniyanandi.sentinel.service;

import com.taniyanandi.sentinel.metrics.SentinelMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@code sentinel.detector.strategy} selects exactly one
 * {@link DetectorStrategy} bean, defaulting to the z-score detector so
 * existing deployments keep their current behaviour.
 */
class DetectorStrategyWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(SentinelMetrics.class, () -> new SentinelMetrics(new SimpleMeterRegistry()))
            .withUserConfiguration(AnomalyDetector.class, EwmaDetector.class);

    @Test
    void defaultsToZScoreStrategy() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(DetectorStrategy.class);
            assertThat(context.getBean(DetectorStrategy.class)).isInstanceOf(AnomalyDetector.class);
        });
    }

    @Test
    void explicitZScoreSelectsZScoreStrategy() {
        runner.withPropertyValues("sentinel.detector.strategy=zscore").run(context -> {
            assertThat(context).hasSingleBean(DetectorStrategy.class);
            assertThat(context.getBean(DetectorStrategy.class)).isInstanceOf(AnomalyDetector.class);
        });
    }

    @Test
    void ewmaPropertySelectsEwmaStrategy() {
        runner.withPropertyValues("sentinel.detector.strategy=ewma").run(context -> {
            assertThat(context).hasSingleBean(DetectorStrategy.class);
            assertThat(context.getBean(DetectorStrategy.class)).isInstanceOf(EwmaDetector.class);
        });
    }
}
