"use client";

import React, { useState } from 'react';
import { Card } from '@/components/ui/card';
import { ThumbsUp, Star } from 'lucide-react';
import Link from 'next/link';
import axios from 'axios';

interface MovieRecommendation {
  movieId: number;
  title: string;
  predictedRating: number;
  genres: string[];
}

interface UserRecommendations {
  userId: number;
  recommendations: MovieRecommendation[];
  generatedAt: string;
  modelVersion: string;
  totalRecommendations: number;
}

const RecommendationsPage: React.FC = () => {
  const [userId, setUserId] = useState<string>('');
  const [limit, setLimit] = useState<string>('10');
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [recommendations, setRecommendations] = useState<UserRecommendations | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!userId || isNaN(parseInt(userId))) {
      setError('Please enter a valid user ID');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const limitParam = limit && !isNaN(parseInt(limit)) ? parseInt(limit) : 10;
      let response;
      if (parseInt(limit) <10){
         response = await axios.get(`http://localhost:8080/recommendations-api/recommendations/users/${userId}/top/${limitParam}`);
      }
      else {
         response = await axios.get(`http://localhost:8080/recommendations-api/recommendations/users/${userId}?limit=${limitParam}`);
      }
      setRecommendations(response.data);
    } catch (err) {
      if (axios.isAxiosError(err)) {
        setError(err.response?.data?.message || err.message || 'API request failed');
      } else {
        setError(err instanceof Error ? err.message : 'An error occurred');
      }
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
          <ThumbsUp className="w-6 h-6 mr-2" />
          Movie Recommendations
        </h1>

        <Card className="p-6 mb-8">
          <h2 className="text-xl font-semibold mb-4">Try the Recommendations API</h2>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label htmlFor="userId" className="block text-sm font-medium mb-1">
                User ID
              </label>
              <input
                id="userId"
                type="number"
                value={userId}
                onChange={(e) => setUserId(e.target.value)}
                className="w-full p-2 border border-slate-300 rounded-md"
                placeholder="Enter user ID"
              />
            </div>

            <div>
              <label htmlFor="limit" className="block text-sm font-medium mb-1">
                Limit (optional)
              </label>
              <input
                id="limit"
                type="number"
                value={limit}
                onChange={(e) => setLimit(e.target.value)}
                className="w-full p-2 border border-slate-300 rounded-md"
                placeholder="Number of recommendations"
              />
            </div>

            <button
              type="submit"
              disabled={loading}
              className="bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 disabled:bg-blue-400"
            >
              {loading ? 'Loading...' : 'Get Recommendations'}
            </button>
          </form>

          {error && (
            <div className="mt-4 p-3 bg-red-100 text-red-700 rounded-md">
              {error}
            </div>
          )}
        </Card>

        {recommendations && (
          <div className="space-y-6">
            <div className="bg-white p-4 rounded-md shadow-sm">
              <h2 className="text-xl font-semibold mb-2">User #{recommendations.userId}</h2>
              <div className="text-sm text-slate-500 mb-4">
                <p>Generated: {new Date(recommendations.generatedAt).toLocaleString()}</p>
                <p>Model Version: {recommendations.modelVersion}</p>
                <p>Total Recommendations: {recommendations.totalRecommendations}</p>
              </div>
            </div>

            <h3 className="text-xl font-semibold">Recommended Movies</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {recommendations.recommendations.map((movie) => (
                <Card key={movie.movieId} className="p-4 hover:shadow-md transition-shadow">
                  <div className="flex justify-between items-start">
                    <h4 className="font-semibold">{movie.title}</h4>
                    <div className="flex items-center bg-yellow-100 px-2 py-1 rounded-md">
                      <Star className="w-4 h-4 text-yellow-500 mr-1" />
                      <span>{movie.predictedRating.toFixed(1)}</span>
                    </div>
                  </div>
                  <div className="mt-2">
                    <p className="text-sm text-slate-500">ID: {movie.movieId}</p>
                    <div className="mt-2 flex flex-wrap gap-1">
                      {movie.genres.map((genre, index) => (
                        <span 
                          key={index} 
                          className="text-xs bg-slate-200 px-2 py-1 rounded-full"
                        >
                          {genre}
                        </span>
                      ))}
                    </div>
                  </div>
                </Card>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default RecommendationsPage;
