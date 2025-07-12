"use client";

import React, { useState } from 'react';
import { Card } from '@/components/ui/card';
import { Activity, Star } from 'lucide-react';
import Link from 'next/link';
import axios from 'axios';

interface InteractionEventRequest {
  userId: number;
  movieId?: number;
  eventType: string;
  timestamp: string;
  rating?: number;
  eventContext?: string;
}

interface InteractionEvent {
  userId: {
    userId: number;
  };
  movieId?: {
    movieId: number;
  };
  details: {
    userId: {
      userId: number;
    };
    eventType: string;
    timestamp: string;
    movieId?: {
      movieId: number;
    };
    ratingValue?: {
      ratingActual: number;
    };
    eventContext?: string;
  };
  processed: boolean;
}

interface MovieRecommendation {
  movieId: {
    movieId: number;
  };
  predictedRating: number;
}

interface EventWithRecommendationsResponse {
  event: InteractionEvent;
  recommendations: MovieRecommendation[];
}


const RealTimeEventsPage: React.FC = () => {
  const [formData, setFormData] = useState<Partial<InteractionEventRequest>>({
    eventType: 'RATING_GIVEN',
    timestamp: new Date().toISOString().slice(0, 23) + 'Z'
  });
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<boolean>(false);
  const [processedEvent, setProcessedEvent] = useState<InteractionEvent | null>(null);
  const [recommendations, setRecommendations] = useState<MovieRecommendation[] | null>(null);

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target;

    // Handle numeric fields
    if (name === 'userId' || name === 'movieId') {
      setFormData({
        ...formData,
        [name]: value ? parseInt(value) : undefined
      });
    } else if (name === 'rating') {
      setFormData({
        ...formData,
        [name]: value ? parseFloat(value) : undefined
      });
    } else {
      setFormData({
        ...formData,
        [name]: value
      });
    }
  };


  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    // Validate required fields
    if (!formData.userId) {
      setError('User ID is required');
      return;
    }

    if (!formData.eventType) {
      setError('Event Type is required');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(false);
    setRecommendations(null); // Clear previous recommendations

    try {
      // Set timestamp to current time if not provided
      const payload = {
        ...formData,
        timestamp: formData.timestamp || new Date().toISOString().slice(0, 23) + 'Z'
      };

      const response = await axios.post(
          '/recommendations-api/recommendations/events/interaction',
          payload
      );

      // Store the processed event and recommendations from the response
      const responseData = response.data as EventWithRecommendationsResponse;
      setProcessedEvent(responseData.event);
      setRecommendations(responseData.recommendations);

      setSuccess(true);
      // Reset optional fields but keep userId for convenience
      setFormData({
        userId: formData.userId,
        eventType: 'RATING_GIVEN',
        timestamp: new Date().toISOString().slice(0, 23) + 'Z'
      });

    } catch (err) {
      if (axios.isAxiosError(err)) {
        setError(err.response?.data?.message || err.message || 'API request failed');
      } else {
        setError(err instanceof Error ? err.message : 'An error occurred');
      }
      setSuccess(false);
    } finally {
      setLoading(false);
    }
  };

  return (
      <div className="min-h-screen bg-gradient-to-br from-slate-50 via-slate-100 to-slate-200 text-slate-800">
        <div className="container mx-auto p-6">
          <div className="mb-6">
            <Link href="/" className="text-blue-600 hover:underline flex items-center">
              ← Back to Analytics
            </Link>
          </div>

          <h1 className="text-3xl font-bold mb-6 flex items-center">
            <Activity className="w-6 h-6 mr-2" />
            Real-Time Events
          </h1>

          <Card className="p-6 mb-8">
            <h2 className="text-xl font-semibold mb-4">Submit Interaction Event</h2>
            <p className="mb-4 text-slate-600">
              Use this form to test the real-time event processing service by submitting user interaction events.
            </p>

            <form onSubmit={handleSubmit} className="space-y-4">
              {/* User ID - Required */}
              <div>
                <label htmlFor="userId" className="block text-sm font-medium mb-1">
                  User ID <span className="text-red-500">*</span>
                </label>
                <input
                    id="userId"
                    name="userId"
                    type="number"
                    value={formData.userId || ''}
                    onChange={handleChange}
                    className="w-full p-2 border border-slate-300 rounded-md"
                    placeholder="Enter user ID"
                    required
                />
              </div>

              {/* Movie ID - Optional */}
              <div>
                <label htmlFor="movieId" className="block text-sm font-medium mb-1">
                  Movie ID
                </label>
                <input
                    id="movieId"
                    name="movieId"
                    type="number"
                    value={formData.movieId || ''}
                    onChange={handleChange}
                    className="w-full p-2 border border-slate-300 rounded-md"
                    placeholder="Enter movie ID (optional)"
                />
              </div>

              {/* Event Type - Required */}
              <div>
                <label htmlFor="eventType" className="block text-sm font-medium mb-1">
                  Event Type <span className="text-red-500">*</span>
                </label>
                <select
                    id="eventType"
                    name="eventType"
                    value={formData.eventType || ''}
                    onChange={handleChange}
                    className="w-full p-2 border border-slate-300 rounded-md"
                    required
                >
                  <option value="RATING_GIVEN">RATING</option>
                  <option value="MOVIE_VIEWED">VIEW</option>
                  <option value="ADDED_TO_WATCHLIST">BOOKMARK</option>
                  <option value="SEARCHED_FOR_MOVIE">SEARCH</option>
                  <option value="MOVIE_CLICKED">CLICK</option>
                </select>
              </div>

              {/* Rating - Optional */}
              <div>
                <label htmlFor="rating" className="block text-sm font-medium mb-1">
                  Rating
                </label>
                <input
                    id="rating"
                    name="rating"
                    type="number"
                    step="0.5"
                    min="0"
                    max="5"
                    value={formData.rating || ''}
                    onChange={handleChange}
                    className="w-full p-2 border border-slate-300 rounded-md"
                    placeholder="Enter rating (0-5)"
                />
              </div>

              {/* Event Context - Optional */}
              <div>
                <label htmlFor="eventContext" className="block text-sm font-medium mb-1">
                  Event Context
                </label>
                <textarea
                    id="eventContext"
                    name="eventContext"
                    value={formData.eventContext || ''}
                    onChange={handleChange}
                    className="w-full p-2 border border-slate-300 rounded-md"
                    placeholder="Additional context (optional)"
                    rows={3}
                />
              </div>

              {/* Timestamp - Hidden, auto-generated */}
              <input
                  type="hidden"
                  name="timestamp"
                  value={formData.timestamp || ''}
              />

              <button
                  type="submit"
                  disabled={loading}
                  className="bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 disabled:bg-blue-400"
              >
                {loading ? 'Submitting...' : 'Submit Event'}
              </button>
            </form>

            {error && (
                <div className="mt-4 p-3 bg-red-100 text-red-700 rounded-md">
                  {error}
                </div>
            )}

            {success && (
                <div className="mt-4 p-3 bg-green-100 text-green-700 rounded-md">
                  Event submitted successfully!
                </div>
            )}

            {processedEvent && (
                <div className="mt-6 p-4 border border-blue-200 rounded-md bg-blue-50">
                  <h3 className="text-lg font-semibold mb-2">Processed Event</h3>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div className="font-medium">User ID:</div>
                    <div>{processedEvent.userId.userId}</div>

                    {processedEvent.movieId && (
                        <React.Fragment key="movie-id">
                          <div className="font-medium">Movie ID:</div>
                          <div>{processedEvent.movieId.movieId}</div>
                        </React.Fragment>
                    )}

                    <div className="font-medium">Event Type:</div>
                    <div>{processedEvent.details.eventType}</div>

                    <div className="font-medium">Timestamp:</div>
                    <div>{processedEvent.details.timestamp}</div>

                    {processedEvent.details.ratingValue && (
                        <React.Fragment key="rating-value">
                          <div className="font-medium">Rating:</div>
                          <div>{processedEvent.details.ratingValue.ratingActual}</div>
                        </React.Fragment>
                    )}

                    {processedEvent.details.eventContext && (
                        <React.Fragment key="event-context">
                          <div className="font-medium">Context:</div>
                          <div>{processedEvent.details.eventContext}</div>
                        </React.Fragment>
                    )}

                    <div className="font-medium">Processed:</div>
                    <div>{processedEvent.processed ? 'Yes' : 'No'}</div>
                  </div>
                </div>
            )}

            {recommendations && recommendations.length > 0 && (
                <div className="mt-6 p-4 border border-green-200 rounded-md bg-green-50">
                  <h3 className="text-lg font-semibold mb-2">Real-Time Recommendations</h3>
                  <p className="text-sm text-slate-600 mb-4">
                    These recommendations were generated in real-time based on the event you just submitted.
                  </p>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    {recommendations.map((movie) => (
                        <div key={movie.movieId.movieId} className="p-3 bg-white rounded-md shadow-sm">
                          <div className="flex justify-between items-start">
                            <h5 className="font-medium">Movie ID: {movie.movieId.movieId}</h5>
                            <div className="flex items-center bg-yellow-100 px-2 py-1 rounded-md">
                              <Star className="w-4 h-4 text-yellow-500 mr-1" />
                              <span>{movie.predictedRating.toFixed(1)}</span>
                            </div>
                          </div>
                        </div>
                    ))}
                  </div>
                </div>
            )}

            {recommendations && recommendations.length === 0 && (
                <div className="mt-6 p-4 border border-yellow-200 rounded-md bg-yellow-50">
                  <h3 className="text-lg font-semibold mb-2">No Recommendations Available</h3>
                  <p className="text-sm text-slate-600">
                    No recommendations were generated for this user or event type.
                    Try submitting a different event or using a different user ID.
                  </p>
                </div>
            )}


          </Card>

          <Card className="p-6">
            <h2 className="text-xl font-semibold mb-4">About Real-Time Events</h2>
            <p className="text-slate-600">
              The real-time event processing service captures user interactions and processes them to generate
              personalized recommendations. Events are sent to a Kafka topic and processed by a Flink job that
              updates the recommendation model in real-time.
            </p>
            <div className="mt-4">
              <h3 className="font-semibold mb-2">Event Types:</h3>
              <ul className="list-disc pl-5 space-y-1 text-slate-600">
                <li><span className="font-medium">RATING</span>: User rates a movie (requires movieId and rating)</li>
                <li><span className="font-medium">VIEW</span>: User views a movie details (requires movieId)</li>
                <li><span className="font-medium">BOOKMARK</span>: User bookmarks a movie (requires movieId)</li>
                <li><span className="font-medium">SEARCH</span>: User searches for movies (context can contain search terms)</li>
                <li><span className="font-medium">CLICK</span>: User clicks on a movie in search results (requires movieId)</li>
              </ul>
            </div>
          </Card>
        </div>
      </div>
  );
};

export default RealTimeEventsPage;