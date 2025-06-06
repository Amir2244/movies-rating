package org.hiast.recommendationsapi.adapter.out.persistence.mongodb;


import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.document.UserRecommendationsDocument;
import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.mapper.RecommendationsDomainMapper;
import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.repository.UserRecommendationsMongoRepository;
import org.hiast.recommendationsapi.application.port.out.RecommendationsRepositoryPort;
import org.hiast.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * MongoDB adapter implementing the RecommendationsRepositoryPort.
 * This is the secondary adapter in hexagonal architecture that handles
 * data persistence concerns while keeping the domain logic clean.
 */
@Repository
public class MongoRecommendationsRepositoryAdapter implements RecommendationsRepositoryPort {
    
    private static final Logger log = LoggerFactory.getLogger(MongoRecommendationsRepositoryAdapter.class);
    
    private final UserRecommendationsMongoRepository mongoRepository;
    private final RecommendationsDomainMapper domainMapper;
    
    /**
     * Constructor for dependency injection.
     *
     * @param mongoRepository The Spring Data MongoDB repository.
     * @param domainMapper    The mapper for converting between documents and domain models.
     */
    public MongoRecommendationsRepositoryAdapter(UserRecommendationsMongoRepository mongoRepository,
                                                RecommendationsDomainMapper domainMapper) {
        this.mongoRepository = Objects.requireNonNull(mongoRepository, "mongoRepository cannot be null");
        this.domainMapper = Objects.requireNonNull(domainMapper, "domainMapper cannot be null");
    }
    
    @Override
    public Optional<UserRecommendations> findByUserId(int userId) {
        Objects.requireNonNull(userId, "userId cannot be null");
        
        log.debug("Searching for recommendations for user: {}", userId);
        
        try {
            Optional<UserRecommendationsDocument> documentOpt = mongoRepository.findByUserId(userId);
            
            if (documentOpt.isPresent()) {
                UserRecommendations domain = domainMapper.toDomain(documentOpt.get());
                log.debug("Found recommendations for user: {} with {} recommendations", 
                    userId, domain.getRecommendations().size());
                return Optional.of(domain);
            } else {
                log.debug("No recommendations found for user: {}", userId);
                return Optional.empty();
            }
        } catch (Exception e) {
            log.error("Error retrieving recommendations for user: {}", userId, e);
            return Optional.empty();
        }
    }
    
    @Override
    public Optional<UserRecommendations> findByUserIdWithLimit(int userId, int limit) {
        Objects.requireNonNull(userId, "userId cannot be null");
        
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, but was: " + limit);
        }
        
        log.debug("Searching for up to {} recommendations for user: {}", limit, userId);
        
        try {
            Optional<UserRecommendationsDocument> documentOpt = mongoRepository.findByUserId(userId);
            
            if (documentOpt.isPresent()) {
                UserRecommendations domain = domainMapper.toDomainWithLimit(documentOpt.get(), limit);
                log.debug("Found {} recommendations for user: {} (limit: {})", 
                    domain.getRecommendations().size(), userId, limit);
                return Optional.of(domain);
            } else {
                log.debug("No recommendations found for user: {}", userId);
                return Optional.empty();
            }
        } catch (Exception e) {
            log.error("Error retrieving recommendations for user: {} with limit: {}", userId, limit, e);
            return Optional.empty();
        }
    }
    
    @Override
    public boolean existsByUserId(int userId) {
        if (userId <= 0) {
            throw new IllegalArgumentException("User ID must be positive, but was: " + userId);
        }
        
        log.debug("Checking if recommendations exist for user: {}", userId);
        
        try {
            boolean exists = mongoRepository.existsByUserId(userId);
            log.debug("Recommendations exist for user {}: {}", userId, exists);
            return exists;
        } catch (Exception e) {
            log.error("Error checking if recommendations exist for user: {}", userId, e);
            return false;
        }
    }
    
    @Override
    public Map<Integer, UserRecommendations> findByUserIds(List<Integer> userIds) {
        Objects.requireNonNull(userIds, "userIds cannot be null");
        
        log.debug("Searching for recommendations for {} users", userIds.size());
        
        try {
            List<UserRecommendationsDocument> documents = mongoRepository.findByUserIdIn(userIds);
            
            Map<Integer, UserRecommendations> result = new HashMap<>();
            for (UserRecommendationsDocument document : documents) {
                Integer userId = document.getUserId();
                UserRecommendations domain = domainMapper.toDomain(document);
                result.put(userId, domain);
            }
            
            log.debug("Found recommendations for {} out of {} requested users", 
                result.size(), userIds.size());
            return result;
        } catch (Exception e) {
            log.error("Error retrieving batch recommendations for users", e);
            return new HashMap<>();
        }
    }
    
    @Override
    public Map<Integer, UserRecommendations> findByUserIdsWithLimit(List<Integer> userIds, int limit) {
        Objects.requireNonNull(userIds, "userIds cannot be null");
        
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, but was: " + limit);
        }
        
        log.debug("Searching for up to {} recommendations for {} users", limit, userIds.size());
        
        try {
            List<UserRecommendationsDocument> documents = mongoRepository.findByUserIdIn(userIds);
            
            Map<Integer, UserRecommendations> result = new HashMap<>();
            for (UserRecommendationsDocument document : documents) {
                Integer userId = document.getUserId();
                UserRecommendations domain = domainMapper.toDomainWithLimit(document, limit);
                result.put(userId, domain);
            }
            
            log.debug("Found recommendations for {} out of {} requested users (limit: {})", 
                result.size(), userIds.size(), limit);
            return result;
        } catch (Exception e) {
            log.error("Error retrieving batch recommendations for users with limit: {}", limit, e);
            return new HashMap<>();
        }
    }
}
