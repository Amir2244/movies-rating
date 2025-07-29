
export const BASE_URL = 'http://35.224.208.17:30565';
export const API_PATH = '/recommendations-api/recommendations/users';

export const TEST_USER_IDS = [1, 42, 100, 500, 1000];
export const TEST_LIMITS = [5, 10, 20, 50];
export const DEFAULT_OPTIONS = {
  vus: 1,
  duration: '10s',

  thresholds: {
    http_req_duration: ['p(95)<2000'],
    http_req_failed: ['rate<0.1'],
  },
  httpDebug: 'full',
};
export const LOAD_TEST_OPTIONS = {
  stages: [
    { duration: '30s', target: 10 },
    { duration: '1m', target: 10 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2500'],
    http_req_failed: ['rate<0.5'],
  },
};

export const STRESS_TEST_OPTIONS = {
  stages: [
    { duration: '30s', target: 10 },
    { duration: '1m', target: 10 },
    { duration: '30s', target: 20 },
    { duration: '1m', target: 20 },
    { duration: '30s', target: 30 },
    { duration: '1m', target: 30 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<2000'],
    http_req_failed: ['rate<0.1'],
  },
};