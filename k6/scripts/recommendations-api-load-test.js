import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

export const BASE_URL = 'http://35.224.208.17:30565';
export const API_PATH = '/recommendations-api/recommendations/users';

export const TEST_USER_IDS = [1, 42, 100, 500, 1000];
export const TEST_LIMITS = [10];
export const LOAD_TEST_OPTIONS = {
  stages: [
    { duration: '30s', target: 10 },
    { duration: '1m', target: 100 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2500'],
    http_req_failed: ['rate<0.5'],
  },
};
const errorRate = new Rate('error_rate');
const recommendationCount = new Trend('recommendation_count');
const requestsMade = new Counter('requests_made');

export const options = LOAD_TEST_OPTIONS;

export default function() {
  const userId = TEST_USER_IDS[Math.floor(Math.random() * TEST_USER_IDS.length)];

  const limit = 10;

  const url = `${BASE_URL}${API_PATH}/${userId}/top/${limit}`;

  const response = http.get(url);
  requestsMade.add(1);

  const success = check(response, {
    'status is 200': (r) => r.status === 200,
    'response has userId': (r) => r.json().userId === userId,
    'response has recommendations': (r) => Array.isArray(r.json().recommendations),
    'response has correct number of recommendations': (r) => {
      const data = r.json();
      return data.recommendations.length <= limit;
    }
  });

  errorRate.add(!success);

  if (success && response.status === 200) {
    const data = response.json();
    recommendationCount.add(data.recommendations.length);

    check(response, {
      'recommendations have expected fields': (r) => {
        const recs = r.json().recommendations;
        return recs.length > 0 && 
               recs.every(rec => 
                 rec.movieId !== undefined && 
                 rec.title !== undefined && 
                 rec.predictedRating !== undefined && 
                 Array.isArray(rec.genres)
               );
      }
    });
  }
  sleep(Math.random() * 2 + 1);
}

export function setup() {
  console.log('Starting load test for recommendations API');
  console.log(`Base URL: ${BASE_URL}`);
  console.log(`API Path: ${API_PATH}`);
  console.log(`Test User IDs: ${TEST_USER_IDS.join(', ')}`);
  console.log(`Test Limits: ${TEST_LIMITS.join(', ')}`);

  const checkUrl = `${BASE_URL}${API_PATH}/${TEST_USER_IDS[0]}/top/1`;
  const checkResponse = http.get(checkUrl);
  
  if (checkResponse.status !== 200) {
    console.error(`API check failed with status ${checkResponse.status}`);
    console.error(checkResponse.body);
  } else {
    console.log('API check successful');
  }
  
  return { startTime: new Date().toISOString() };
}

export function teardown(data) {
  console.log(`Load test completed. Started at: ${data.startTime}, Ended at: ${new Date().toISOString()}`);
}