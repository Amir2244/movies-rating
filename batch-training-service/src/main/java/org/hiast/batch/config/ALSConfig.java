package org.hiast.batch.config;

public class ALSConfig {
    private final int rank;
    private final int maxIter;
    private final double regParam;
    private final long seed;
    private final boolean implicitPrefs;
    private final double alpha;
    private final double trainingSplitRatio;

    public ALSConfig(int rank, int maxIter, double regParam, long seed, boolean implicitPrefs, double alpha, double trainingSplitRatio) {
        this.rank = rank;
        this.maxIter = maxIter;
        this.regParam = regParam;
        this.seed = seed;
        this.implicitPrefs = implicitPrefs;
        this.alpha = alpha;
        this.trainingSplitRatio = trainingSplitRatio;
    }

    public int getRank() {
        return rank;
    }

    public int getMaxIter() {
        return maxIter;
    }

    public double getRegParam() {
        return regParam;
    }

    public long getSeed() {
        return seed;
    }

    public boolean isImplicitPrefs() {
        return implicitPrefs;
    }

    public double getAlpha() {
        return alpha;
    }

    @Override
    public String toString() {
        return "ALSConfig{" +
                "rank=" + rank +
                ", maxIter=" + maxIter +
                ", regParam=" + regParam +
                ", seed=" + seed +
                ", implicitPrefs=" + implicitPrefs +
                ", alpha=" + alpha +
                '}';
    }

    public double getTrainingSplitRatio() {
        return trainingSplitRatio;
    }
}