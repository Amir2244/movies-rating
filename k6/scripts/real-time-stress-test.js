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
        movieId: movieId,
        eventType: eventType,
        rating: 5,
        timestamp: new Date().toISOString(),
    };

    const url = `${BASE_URL}${API_PATH}`;
    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const res = http.post(url, JSON.stringify(payload), params);

    requestsMade.add(1);
    const success = check(res, {
        'status is 202 (Accepted)': (r) => r.status === 202,
        'response includes a processed event': (r) => {
            if (r.status !== 202) return false;
            try {
                const event = r.json('event');
                return event && event.processed === true;
            } catch (e) {
                return false;
            }
        },
        'response recommendations are valid (array or null)': (r) => {
            if (r.status !== 202) return false;
            try {
                const recommendations = r.json('recommendations');
                return recommendations === null || Array.isArray(recommendations);
            } catch(e) {
                return false;
            }
        },
    });

    errorRate.add(!success);
    eventProcessingTime.add(res.timings.duration);

    if (!success) {
        console.log(`Request failed validation. Status: ${res.status}, Body: ${res.body}`);
    }

    sleep(1);
    activeUsers.add(-1);
}