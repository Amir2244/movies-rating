package org.hiast.recommendationsapi.adapter.in.web;

import org.hiast.events.EventType;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.UserRecommendations;
import org.hiast.recommendationsapi.adapter.in.web.mapper.RecommendationsResponseMapper;
import org.hiast.recommendationsapi.adapter.in.web.request.InteractionEventRequest;
import org.hiast.recommendationsapi.adapter.in.web.response.EventWithRecommendationsResponse;
import org.hiast.recommendationsapi.adapter.in.web.response.UserRecommendationsResponse;
import org.hiast.recommendationsapi.application.port.in.GetBatchUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.port.in.GetUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.service.RealTimeEventService;
import org.hiast.recommendationsapi.domain.exception.InvalidRecommendationRequestException;
import org.hiast.recommendationsapi.domain.exception.UserRecommendationsNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RecommendationsController.
 */
@ExtendWith(MockitoExtension.class)
class RecommendationsControllerTest {

    @Mock
    private GetUserRecommendationsUseCase getUserRecommendationsUseCase;

    @Mock
    private GetBatchUserRecommendationsUseCase getBatchUserRecommendationsUseCase;

    @Mock
    private RecommendationsResponseMapper responseMapper;

    @Mock
    private RealTimeEventService realTimeEventService;

    private RecommendationsController controller;

    @BeforeEach
    void setUp() {
        controller = new RecommendationsController(
            getUserRecommendationsUseCase,
            getBatchUserRecommendationsUseCase,
            responseMapper,
            realTimeEventService
        );
    }

    @Test
    void getUserRecommendations_WhenRecommendationsExist_ShouldReturnOk() {
        // Given
        int userId = 1;
        UserRecommendations userRecommendations = mock(UserRecommendations.class);
        UserRecommendationsResponse response = mock(UserRecommendationsResponse.class);

        when(getUserRecommendationsUseCase.getUserRecommendations(userId))
            .thenReturn(Optional.of(userRecommendations));
        when(responseMapper.toResponse(userRecommendations))
            .thenReturn(response);

        // When
        ResponseEntity<UserRecommendationsResponse> result = controller.getUserRecommendations(userId);

        // Then
        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals(response, result.getBody());
        verify(getUserRecommendationsUseCase).getUserRecommendations(userId);
        verify(responseMapper).toResponse(userRecommendations);
    }

    @Test
    void getUserRecommendations_WhenNoRecommendationsExist_ShouldReturnNotFound() {
        // Given
        int userId = 1;
        when(getUserRecommendationsUseCase.getUserRecommendations(userId))
            .thenReturn(Optional.empty());

        // When
        ResponseEntity<UserRecommendationsResponse> response = controller.getUserRecommendations(userId);

        // Then
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
        verify(getUserRecommendationsUseCase).getUserRecommendations(userId);
        verifyNoInteractions(responseMapper);
    }

    @Test
    void getUserRecommendationsWithLimit_WhenRecommendationsExist_ShouldReturnOk() {
        // Given
        int userId = 1;
        int limit = 5;
        UserRecommendations userRecommendations = mock(UserRecommendations.class);
        UserRecommendationsResponse response = mock(UserRecommendationsResponse.class);

        when(getUserRecommendationsUseCase.getUserRecommendations(userId, limit))
            .thenReturn(Optional.of(userRecommendations));
        when(responseMapper.toResponse(userRecommendations))
            .thenReturn(response);

        // When
        ResponseEntity<UserRecommendationsResponse> result = controller.getUserRecommendationsWithLimit(userId, limit);

        // Then
        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertEquals(response, result.getBody());
        verify(getUserRecommendationsUseCase).getUserRecommendations(userId, limit);
        verify(responseMapper).toResponse(userRecommendations);
    }

    @Test
    void getBatchUserRecommendations_WhenValidInput_ShouldReturnOk() {
        // Given
        String userIdsString = "1,2,3";
        List<Integer> userIds = Arrays.asList(1, 2, 3);

        Map<Integer, UserRecommendations> userRecommendationsMap = new HashMap<>();
        userRecommendationsMap.put(1, mock(UserRecommendations.class));
        userRecommendationsMap.put(2, mock(UserRecommendations.class));

        Map<String, UserRecommendationsResponse> responseMap = new HashMap<>();
        responseMap.put("1", mock(UserRecommendationsResponse.class));
        responseMap.put("2", mock(UserRecommendationsResponse.class));

        when(getBatchUserRecommendationsUseCase.getBatchUserRecommendations(anyList()))
            .thenReturn(userRecommendationsMap);

        when(responseMapper.toResponse(any(UserRecommendations.class)))
            .thenReturn(mock(UserRecommendationsResponse.class));

        // When
        ResponseEntity<Map<String, UserRecommendationsResponse>> result = 
            controller.getBatchUserRecommendations(userIdsString);

        // Then
        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertNotNull(result.getBody());
        assertEquals(2, result.getBody().size());
        verify(getBatchUserRecommendationsUseCase).getBatchUserRecommendations(anyList());
    }

    @Test
    void getBatchUserRecommendationsWithLimit_WhenValidInput_ShouldReturnOk() {
        // Given
        String userIdsString = "1,2,3";
        int limit = 5;
        List<Integer> userIds = Arrays.asList(1, 2, 3);

        Map<Integer, UserRecommendations> userRecommendationsMap = new HashMap<>();
        userRecommendationsMap.put(1, mock(UserRecommendations.class));
        userRecommendationsMap.put(2, mock(UserRecommendations.class));

        Map<String, UserRecommendationsResponse> responseMap = new HashMap<>();
        responseMap.put("1", mock(UserRecommendationsResponse.class));
        responseMap.put("2", mock(UserRecommendationsResponse.class));

        when(getBatchUserRecommendationsUseCase.getBatchUserRecommendations(anyList(), eq(limit)))
            .thenReturn(userRecommendationsMap);

        when(responseMapper.toResponse(any(UserRecommendations.class)))
            .thenReturn(mock(UserRecommendationsResponse.class));

        // When
        ResponseEntity<Map<String, UserRecommendationsResponse>> result = 
            controller.getBatchUserRecommendationsWithLimit(userIdsString, limit);

        // Then
        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertNotNull(result.getBody());
        assertEquals(2, result.getBody().size());
        verify(getBatchUserRecommendationsUseCase).getBatchUserRecommendations(anyList(), eq(limit));
    }

    @Test
    void submitInteractionEventWithRecommendations_WhenValidInput_ShouldReturnAccepted() {
        // Given
        InteractionEventRequest request = mock(InteractionEventRequest.class);
        RealTimeEventService.EventWithRecommendations eventWithRecommendations = mock(RealTimeEventService.EventWithRecommendations.class);
        org.hiast.model.InteractionEvent event = mock(org.hiast.model.InteractionEvent.class);
        List<MovieRecommendation> recommendations = Arrays.asList(mock(MovieRecommendation.class));

        when(request.getUserId()).thenReturn(1);
        when(realTimeEventService.processInteractionEventWithRecommendations(request))
            .thenReturn(eventWithRecommendations);
        when(eventWithRecommendations.getEvent()).thenReturn(event);
        when(eventWithRecommendations.getRecommendations()).thenReturn(recommendations);

        // When
        ResponseEntity<EventWithRecommendationsResponse> result = 
            controller.submitInteractionEventWithRecommendations(request);

        // Then
        assertEquals(HttpStatus.ACCEPTED, result.getStatusCode());
        assertNotNull(result.getBody());
        verify(realTimeEventService).processInteractionEventWithRecommendations(request);
        verify(eventWithRecommendations).getEvent();
        // Using atLeastOnce() because the method calls getRecommendations multiple times
        verify(eventWithRecommendations, atLeastOnce()).getRecommendations();
    }
}
