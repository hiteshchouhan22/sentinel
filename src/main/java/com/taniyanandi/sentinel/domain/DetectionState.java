package com.taniyanandi.sentinel.domain;

import java.io.Serializable;

public class DetectionState implements Serializable {
    private long count;
    private double mean;
    private double m2;
    private long ewmaCount;
    private double ewmaMean;
    private double ewmaVariance;

    public DetectionState() {}

    public void observe(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        m2 += delta * delta2;
    }

    /**
     * Updates the EWMA baseline with one observation, using the incremental
     * form of West (1979), the exponentially weighted analogue of Welford's
     * algorithm used by {@link #observe(double)}:
     * <pre>
     *   delta        = value - ewmaMean
     *   ewmaMean    += alpha * delta
     *   ewmaVariance = (1 - alpha) * (ewmaVariance + alpha * delta^2)
     * </pre>
     * The first observation initialises the mean and leaves the variance at
     * zero, so a detector stays in warm-up until the stream shows variation.
     */
    public void observeEwma(double value, double alpha) {
        if (ewmaCount == 0) {
            ewmaMean = value;
            ewmaVariance = 0.0;
        } else {
            double delta = value - ewmaMean;
            ewmaMean += alpha * delta;
            ewmaVariance = (1.0 - alpha) * (ewmaVariance + alpha * delta * delta);
        }
        ewmaCount++;
    }

    public long count() { return count; }
    public double mean() { return mean; }
    public double variance() { return count > 1 ? m2 / (count - 1) : 0.0; }
    public double stdDev() { return Math.sqrt(variance()); }
    public long ewmaCount() { return ewmaCount; }
    public double ewmaMean() { return ewmaMean; }
    public double ewmaVariance() { return ewmaVariance; }
    public double ewmaStdDev() { return Math.sqrt(ewmaVariance); }
}
