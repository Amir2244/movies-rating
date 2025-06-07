package org.hiast.batch.config;

public class HDFSConfig {
    private final String ratingsPath;
    private final String modelSavePath;

    public HDFSConfig(String ratingsPath, String modelSavePath) {
        this.ratingsPath = ratingsPath;
        this.modelSavePath = modelSavePath;
    }

    public String getRatingsPath() { return ratingsPath; }

    public String getModelSavePath() { return modelSavePath; }

    @Override
    public String toString() {
        return "HDFSConfig{" +
                "ratingsPath='" + ratingsPath + '\'' +
                ", modelSavePath='" + modelSavePath + '\'' +
                '}';
    }
}
