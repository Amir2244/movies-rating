package org.hiast.batch.application.port.out;


import org.hiast.batch.domain.model.ModelFactors;

/**
 * Output Port defining how to persist the computed user and item factors.
 * Implementations (adapters) will handle specific persistence stores like Redis.
 */
public interface FactorPersistencePort {
    /**
     * Saves the user and item factors.
     *
     * @param modelFactors The ModelFactors object containing user and item factor Datasets.
     */
    void saveModelFactors(ModelFactors modelFactors);
}