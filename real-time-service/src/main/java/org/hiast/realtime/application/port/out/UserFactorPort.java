package org.hiast.realtime.application.port.out;

import org.hiast.ids.UserId;
import org.hiast.model.factors.UserFactor;
import java.util.Optional;

/**
 * Output Port for fetching user factors.
 * The adapter will implement this to get data from Redis.
 */
public interface UserFactorPort {
    Optional<UserFactor> findUserFactorById(UserId userId);
}
