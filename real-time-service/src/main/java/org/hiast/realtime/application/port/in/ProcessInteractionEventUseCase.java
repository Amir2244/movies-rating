package org.hiast.realtime.application.port.in;

import org.hiast.realtime.domain.model.InteractionEvent;

/**
 * Input Port (Use Case Interface).
 * Defines the primary entry point for the application's core logic.
 * A Flink job will call this use case.
 */
public interface ProcessInteractionEventUseCase {
    void processEvent(InteractionEvent event);
}
