import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter, Gauge } from 'k6/metrics';

export const BASE_URL = 'http://35.224.208.17:30565';
export const API_PATH = '/recommendations-api/recommendations/events/interaction';
export const TEST_USER_IDS = [1, 42, 100, 250, 500, 750, 1000];
export const TEST_MOVIE_IDS = [101, 202, 303, 404, 505, 606, 707];
export const EVENT_TYPES = ['RATING_GIVEN'];

export const STRESS_TEST_OPTIONS = {
    stages: [
        { duration: '30s', target: 50 },
        { duration: '1m', target: 50 },
        { duration: '30s', target: 150 },
        { duration: '1m', target: 150 },
        { duration: '30s', target: 250 },
        { duration: '1m', target: 250 },
        { duration: '30s', target: 0 },
    ],
    thresholds: {
        http_req_duration: ['p(95)<10000'],
        http_req_failed: ['rate<0.05'],
    },
};

export const options = STRESS_TEST_OPTIONS;
const errorRate = new Rate('error_rate');
const requestsMade = new Counter('requests_made');
const eventProcessingTime = new Trend('event_processing_time');
const activeUsers = new Gauge('active_users');
export default function() {
    activeUsers.add(1);
    const userId = TEST_USER_IDS[Math.floor(Math.random() * TEST_USER_IDS.length)];
    const movieId = TEST_MOVIE_IDS[Math.floor(Math.random() * TEST_MOVIE_IDS.length)];
    const eventType = 'RATING_GIVEN';

    const payload = {
        userId: userId,
        eventType: eventType,
        timestamp: new Date().toISOString(),
    };

    if (['RATING_GIVEN', 'MOVIE_VIEWED', 'ADDED_TO_WATCHLIST', 'MOVIE_CLICKED'].includes(eventType)) {
        payload.movieId = movieId;
    }
    if (eventType === 'RATING_GIVEN') {
        payload.rating = "5";
    }

    const url = `${BASE_URL}${API_PATH}`;
    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
        tags: {
            eventType: eventType,
            testType: 'stress'
        }
    };
    const response = http.post(url, JSON.stringify(payload), params);
    requestsMade.add(1);
    eventProcessingTime.add(response.timings.duration);

    const success = check(response, {
        'status is 200': (r) => r.status === 202,
        'response body is valid JSON': (r) => {
            try {
                r.json();
                return true;
            } catch (e) {
                console.error(`Failed to parse JSON for event ${eventType}: ${e.message}`);
                return false;
            }
        },
        'response contains event and recommendations': (r) => {
            const body = r.json();
            return body && body.event && Array.isArray(body.recommendations);
        }
    });
    errorRate.add(!success);
    if (!success) {
        console.error(`Request failed: Event ${eventType}, Status ${response.status}, User ${userId}`);
    }
    sleep(Math.random() * 1.5 + 0.5);
    activeUsers.add(-1);
}
export function setup() {
    console.log('Starting Real-Time Service Stress Test...');
    console.log(`Target URL: ${BASE_URL}${API_PATH}`);
}

export function teardown(data) {
    console.log('Real-Time Service Stress Test completed.');
}