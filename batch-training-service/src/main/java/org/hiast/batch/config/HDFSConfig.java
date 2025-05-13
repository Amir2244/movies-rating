package org.hiast.batch.config;

public class HDFSConfig {
    private final String ratingsPath;

    public HDFSConfig(String ratingsPath) {
        this.ratingsPath = ratingsPath;
    }

    public String getRatingsPath() { return ratingsPath; }

    @Override
    public String toString() {
        return "HDFSConfig{" +
                "ratingsPath='" + ratingsPath + '\'' +
                '}';
    }
}