package org.hiast.realtime.adapter.in.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.hiast.realtime.application.port.in.ProcessInteractionEventUseCase;
import org.hiast.realtime.domain.model.InteractionEvent;

/**
 * Flink MapFunction that acts as a bridge between the Flink DataStream
 * and our application's use case.
 */
public class RecommendationMapFunction implements MapFunction<InteractionEvent, Void> {

    private final ProcessInteractionEventUseCase processInteractionEventUseCase;

    public RecommendationMapFunction(ProcessInteractionEventUseCase processInteractionEventUseCase) {
        this.processInteractionEventUseCase = processInteractionEventUseCase;
    }

    @Override
    public Void map(InteractionEvent event) throws Exception {
        processInteractionEventUseCase.processEvent(event);
        return null;
    }
}
