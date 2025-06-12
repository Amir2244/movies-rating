# Analytics API - Usage Examples

This document provides comprehensive examples of how to use the Analytics API endpoints.

## Base URL
```
http://localhost:8083/api/v1/analytics
```

## Available Endpoints

### 1. Get Analytics Data (with filters and pagination)

#### Basic Usage
```bash
# Get all analytics (first 20 records)
curl "http://localhost:8083/api/v1/analytics"

# Get analytics with pagination
curl "http://localhost:8083/api/v1/analytics?page=0&size=10"
```

#### Filter by Analytics Types
```bash
# Filter by single type
curl "http://localhost:8083/api/v1/analytics?types=USER_ACTIVITY"

# Filter by multiple types
curl "http://localhost:8083/api/v1/analytics?types=USER_ACTIVITY,MOVIE_POPULARITY,RATING_DISTRIBUTION"

# Get only data quality analytics
curl "http://localhost:8083/api/v1/analytics?types=DATA_COMPLETENESS,DATA_FRESHNESS,DATA_QUALITY"
```

#### Filter by Date Range
```bash
# Get analytics from last month
curl "http://localhost:8083/api/v1/analytics?fromDate=2024-01-01T00:00:00&toDate=2024-01-31T23:59:59"

# Get analytics from last week
curl "http://localhost:8083/api/v1/analytics?fromDate=2024-01-15T00:00:00"

# Get analytics up to a certain date
curl "http://localhost:8083/api/v1/analytics?toDate=2024-01-15T23:59:59"
```

#### Search by Keyword
```bash
# Search analytics descriptions for keywords
curl "http://localhost:8083/api/v1/analytics?keyword=user%20engagement"

# Search for performance-related analytics
curl "http://localhost:8083/api/v1/analytics?keyword=performance"
```

#### Sorting
```bash
# Sort by generation date (newest first) - default
curl "http://localhost:8083/api/v1/analytics?sortBy=generatedAt&sortDirection=DESC"

# Sort by analytics type
curl "http://localhost:8083/api/v1/analytics?sortBy=type&sortDirection=ASC"

# Sort by analytics ID
curl "http://localhost:8083/api/v1/analytics?sortBy=analyticsId&sortDirection=ASC"
```

#### Complex Queries
```bash
# Combine multiple filters
curl "http://localhost:8083/api/v1/analytics?types=USER_ACTIVITY,CONTENT_PERFORMANCE&fromDate=2024-01-01T00:00:00&page=0&size=5&sortBy=generatedAt&sortDirection=DESC"

# Search with pagination and type filter
curl "http://localhost:8083/api/v1/analytics?types=RATING_DISTRIBUTION&keyword=distribution&page=1&size=10"
```

### 2. Get Specific Analytics by ID

```bash
# Get analytics by specific ID
curl "http://localhost:8083/api/v1/analytics/user_activity_12345"

# Get analytics by another ID
curl "http://localhost:8083/api/v1/analytics/movie_popularity_67890"
```

### 3. Get Available Analytics Types

```bash
# Get all analytics types with counts
curl "http://localhost:8083/api/v1/analytics/types"
```

**Sample Response:**
```json
[
  {
    "type": "USER_ACTIVITY",
    "category": "User Behavior",
    "description": "Tracks user rating patterns and activity levels",
    "count": 45
  },
  {
    "type": "MOVIE_POPULARITY",
    "category": "Content Analytics",
    "description": "Tracks movie popularity and rating frequency",
    "count": 32
  }
]
```

### 4. Get Analytics Summary

```bash
# Get comprehensive analytics summary
curl "http://localhost:8083/api/v1/analytics/summary"
```

**Sample Response:**
```json
{
  "totalAnalytics": 1250,
  "uniqueTypes": 12,
  "latestAnalyticsDate": "2024-01-15T10:30:00Z",
  "oldestAnalyticsDate": "2024-01-01T00:00:00Z",
  "categorySummaries": [
    {
      "category": "User Behavior",
      "count": 350,
      "percentage": 28.0
    },
    {
      "category": "Content Analytics",
      "count": 400,
      "percentage": 32.0
    },
    {
      "category": "Data Quality",
      "count": 300,
      "percentage": 24.0
    },
    {
      "category": "System Performance",
      "count": 200,
      "percentage": 16.0
    }
  ]
}
```

### 5. Get All Analytics Type Enums

```bash
# Get all possible analytics types
curl "http://localhost:8083/api/v1/analytics/types/enum"
```

### 6. Health Check

```bash
# Check API health
curl "http://localhost:8083/api/v1/analytics/health"
```

## Response Format

### Paginated Response
```json
{
  "analytics": [
    {
      "analyticsId": "user_activity_12345",
      "generatedAt": "2024-01-15T10:30:00Z",
      "type": "USER_ACTIVITY",
      "description": "User activity and engagement analytics",
      "metrics": {
        "totalUsers": 15420,
        "activeUsers": 8934,
        "averageRatingsPerUser": 12.5,
        "userEngagementScore": 0.58
      },
      "metricsCount": 4
    }
  ],
  "totalElements": 150,
  "totalPages": 15,
  "currentPage": 0,
  "pageSize": 10,
  "hasNext": true,
  "hasPrevious": false
}
```

### Individual Analytics Record
```json
{
  "analyticsId": "movie_popularity_67890",
  "generatedAt": "2024-01-15T10:30:00Z",
  "type": "MOVIE_POPULARITY",
  "description": "Movie popularity and rating frequency analytics",
  "metrics": {
    "totalMovies": 5000,
    "topRatedMovies": 250,
    "averageRating": 3.8,
    "popularityScore": 0.75,
    "ratingFrequency": 1250000
  },
  "metricsCount": 5
}
```

## Error Responses

### Bad Request (400)
```json
{
  "code": "INVALID_ARGUMENT",
  "message": "Invalid analytics type: UNKNOWN_TYPE",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Not Found (404)
```json
{
  "code": "NOT_FOUND",
  "message": "Analytics with ID 'invalid_id' not found",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Internal Server Error (500)
```json
{
  "code": "INTERNAL_ERROR",
  "message": "An unexpected error occurred",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## JavaScript/Frontend Examples

### Using Fetch API
```javascript
// Get analytics with filters
async function getAnalytics() {
    const response = await fetch(
        'http://localhost:8083/api/v1/analytics?types=USER_ACTIVITY&page=0&size=10'
    );
    const data = await response.json();
    console.log(data);
}

// Get analytics summary
async function getAnalyticsSummary() {
    const response = await fetch('http://localhost:8083/api/v1/analytics/summary');
    const summary = await response.json();
    console.log('Total Analytics:', summary.totalAnalytics);
    console.log('Categories:', summary.categorySummaries);
}
```

### Using Axios
```javascript
import axios from 'axios';

// Get analytics with error handling
async function fetchAnalytics() {
    try {
        const response = await axios.get('http://localhost:8083/api/v1/analytics', {
            params: {
                types: ['USER_ACTIVITY', 'MOVIE_POPULARITY'],
                page: 0,
                size: 20,
                sortBy: 'generatedAt',
                sortDirection: 'DESC'
            }
        });
        return response.data;
    } catch (error) {
        console.error('Error fetching analytics:', error.response.data);
        throw error;
    }
}
```

## Python Examples

### Using Requests
```python
import requests
from datetime import datetime, timedelta

# Get analytics from last 7 days
def get_recent_analytics():
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    params = {
        'fromDate': start_date.isoformat(),
        'toDate': end_date.isoformat(),
        'page': 0,
        'size': 50
    }
    
    response = requests.get('http://localhost:8083/api/v1/analytics', params=params)
    return response.json()

# Get analytics summary
def get_analytics_summary():
    response = requests.get('http://localhost:8083/api/v1/analytics/summary')
    return response.json()
```

## Analytics Types Reference

### Data Quality
- `DATA_COMPLETENESS`: Tracks completeness and integrity of rating data
- `DATA_FRESHNESS`: Monitors freshness and recency of rating data  
- `DATA_QUALITY`: Comprehensive data quality metrics

### User Behavior
- `USER_ACTIVITY`: Tracks user rating patterns and activity levels
- `USER_ENGAGEMENT`: Measures user engagement depth and interaction quality
- `USER_SEGMENTATION`: Segments users based on rating behavior patterns

### Content Analytics
- `MOVIE_POPULARITY`: Tracks movie popularity and rating frequency
- `CONTENT_PERFORMANCE`: Analyzes overall content performance metrics
- `RATING_DISTRIBUTION`: Analyzes distribution of rating values
- `GENRE_DISTRIBUTION`: Tracks genre preferences and distribution patterns
- `TEMPORAL_TRENDS`: Identifies temporal trends and seasonal patterns

### System Performance
- `PROCESSING_PERFORMANCE`: Monitors batch processing performance metrics

### Business Metrics
- `USER_SATISFACTION`: Tracks user satisfaction with recommendations

## Best Practices

1. **Use Pagination**: Always specify page size to avoid large responses
2. **Filter by Type**: Use type filters to get only relevant analytics
3. **Date Range Filtering**: Use date ranges for time-series analysis
4. **Caching**: Results are cached, so repeated identical queries are fast
5. **Error Handling**: Always handle potential 404 and 400 errors
6. **Sorting**: Use sorting to get the most relevant results first 