import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter, Gauge } from 'k6/metrics';

export const BASE_URL = 'http://35.224.208.17:30565';
export const API_PATH = '/recommendations-api/recommendations/users';

export const TEST_USER_IDS = [1, 42, 100, 500, 1000];
export const TEST_LIMITS = [10];
export const STRESS_TEST_OPTIONS = {
  stages: [
    { duration: '30s', target: 100 },
    { duration: '1m', target: 100 },
    { duration: '30s', target: 200 },
    { duration: '1m', target: 200 },
    { duration: '30s', target: 300 },
    { duration: '1m', target: 300 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2500'],
    http_req_failed: ['rate<0.1'],
  },
};

const errorRate = new Rate('error_rate');
const recommendationCount = new Trend('recommendation_count');
const requestsMade = new Counter('requests_made');
const activeUsers = new Gauge('active_users');

export const options = STRESS_TEST_OPTIONS;
export default function() {
  activeUsers.add(1);
  const userId = TEST_USER_IDS[Math.floor(Math.random() * TEST_USER_IDS.length)];
  const limit = 10;
  const url = `${BASE_URL}${API_PATH}/${userId}/top/${limit}`;
  const params = {
    tags: {
      userId: userId.toString(),
      limit: limit.toString(),
      testType: 'stress'
    }
  };
  const startTime = new Date().getTime();
  const response = http.get(url, params);
  const endTime = new Date().getTime();

  requestsMade.add(1);

  const responseTime = endTime - startTime;

  const success = check(response, {
    'status is 200': (r) => r.status === 200,
    'response time < 2500ms': (r) => responseTime < 2500,
    'response has userId': (r) => {
      try {
        return r.json().userId === userId;
      } catch (e) {
        console.error(`Failed to parse JSON: ${e.message}`);
        return false;
      }
    },
    'response has recommendations': (r) => {
      try {
        return Array.isArray(r.json().recommendations);
      } catch (e) {
        return false;
      }
    }
  });
  errorRate.add(!success);

  if (success && response.status === 200) {
    try {
      const data = response.json();
      recommendationCount.add(data.recommendations.length);
      if (Math.random() < 0.1) {
        console.log(`User ${userId}, Limit ${limit}: Got ${data.recommendations.length} recommendations in ${responseTime}ms`);
      }
    } catch (e) {
      console.error(`Failed to process successful response: ${e.message}`);
    }
  } else {
    console.error(`Request failed: User ${userId}, Limit ${limit}, Status ${response.status}, Time ${responseTime}ms`);
    if (response.body) {
      try {
        console.error(`Response body: ${response.body.substring(0, 200)}...`);
      } catch (e) {
        console.error(`Could not log response body: ${e.message}`);
      }
    }
  }
  sleep(Math.random() * 0.5 + 0.1);
  activeUsers.add(-1);
}

export function setup() {
  console.log('Starting STRESS TEST for recommendations API');
  console.log(`Base URL: ${BASE_URL}`);
  console.log(`API Path: ${API_PATH}`);
  console.log(`Test User IDs: ${TEST_USER_IDS.join(', ')}`);
  console.log(`Test Limits: ${TEST_LIMITS.join(', ')}`);
  console.log('WARNING: This is a stress test and may put significant load on the system!');

  const checkUrl = `${BASE_URL}${API_PATH}/${TEST_USER_IDS[0]}/top/1`;
  const checkResponse = http.get(checkUrl);
  
  if (checkResponse.status !== 200) {
    console.error(`API check failed with status ${checkResponse.status}`);
    console.error(checkResponse.body);
  } else {
    console.log('API check successful, proceeding with stress test');
  }
  
  return { 
    startTime: new Date().toISOString(),
    testConfig: STRESS_TEST_OPTIONS
  };
}
export function teardown(data) {
  console.log(`Stress test completed. Started at: ${data.startTime}, Ended at: ${new Date().toISOString()}`);
  console.log('Please check system metrics to ensure the service has recovered after the stress test.');
}