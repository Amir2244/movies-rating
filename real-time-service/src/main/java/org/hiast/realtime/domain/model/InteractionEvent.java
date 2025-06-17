package org.hiast.realtime.domain.model;

import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEventDetails;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a user interaction event consumed from Kafka.
 * This is the core domain object for our real-time processing.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InteractionEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private UserId userId;
    private MovieId movieId;
    private InteractionEventDetails details;
}
