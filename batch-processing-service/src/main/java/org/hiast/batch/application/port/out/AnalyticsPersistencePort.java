package org.hiast.batch.application.port.out;

import org.hiast.model.DataAnalytics;

/**
 * Output Port defining how to persist analytics data.
 * Implementations (adapters) will handle specific persistence stores like MongoDB.
 */
public interface AnalyticsPersistencePort {
    /**
     * Saves data analytics to the persistence store.
     *
     * @param dataAnalytics The analytics data to save.
     * @return True if the save operation was successful, false otherwise.
     */
    boolean saveDataAnalytics(DataAnalytics dataAnalytics);
}
