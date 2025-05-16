package org.hiast.batch.application.pipeline.filters;

import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.pipeline.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultSavingFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(ResultSavingFilter.class);

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext input) {
        return null;
    }
}
