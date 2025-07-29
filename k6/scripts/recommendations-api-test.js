import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Rate, Trend } from 'k6/metrics';

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
}
const errorRate = new Rate('error_rate');
const recommendationCount = new Trend('recommendation_count');

export const options = DEFAULT_OPTIONS;

export default function() {
  group('Test recommendations with default limit', () => {
    for (const userId of TEST_USER_IDS) {
      const url = `${BASE_URL}${API_PATH}/${userId}/top/10`;
      const response = http.get(url);
      const success = check(response, {
        'status is 200': (r) => r.status === 200,
        'response has userId': (r) => r.json().userId === userId,
        'response has recommendations': (r) => Array.isArray(r.json().recommendations),
        'response has generatedAt': (r) => r.json().generatedAt !== undefined,
        'response has modelVersion': (r) => r.json().modelVersion !== undefined,
        'response has totalRecommendations': (r) => r.json().totalRecommendations !== undefined,
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
          },
          'predictedRating is between 0 and 5': (r) => {
            const recs = r.json().recommendations;
            return recs.length > 0 && 
                   recs.every(rec => 
                     rec.predictedRating >= 0 && 
                     rec.predictedRating <= 5
                   );
          }
        });
      }
      sleep(1);
    }
  });

  group('Test recommendations with different limits', () => {
    const userId = TEST_USER_IDS[0];
    
    for (const limit of TEST_LIMITS) {
      const url = `${BASE_URL}${API_PATH}/${userId}/top/${limit}`;

      const response = http.get(url);

      const success = check(response, {
        'status is 200': (r) => r.status === 200,
        'response has correct number of recommendations': (r) => {
          const data = r.json();
          return data.recommendations.length <= limit;
        }
      });

      errorRate.add(!success);

      if (success && response.status === 200) {
        const data = response.json();
        recommendationCount.add(data.recommendations.length);
        
        console.log(`User ${userId} with limit ${limit}: Got ${data.recommendations.length} recommendations`);
      }
      sleep(1);
    }
  });

  group('Test error handling', () => {
    const invalidUserUrl = `${BASE_URL}${API_PATH}/invalid/top/10`;
    const invalidUserResponse = http.get(invalidUserUrl);
    
    check(invalidUserResponse, {
      'invalid user ID returns 4xx status': (r) => r.status >= 400 && r.status < 500,
    });

    const invalidLimitUrl = `${BASE_URL}${API_PATH}/1/top/invalid`;
    const invalidLimitResponse = http.get(invalidLimitUrl);
    
    check(invalidLimitResponse, {
      'invalid limit returns 4xx status': (r) => r.status >= 400 && r.status < 500,
    });

    const negativeLimitUrl = `${BASE_URL}${API_PATH}/1/top/-10`;
    const negativeLimitResponse = http.get(negativeLimitUrl);
    
    check(negativeLimitResponse, {
      'negative limit returns 4xx status': (r) => r.status >= 400 && r.status < 500,
    });

    sleep(1);
  });
}