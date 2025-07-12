"use client";

/**
 * @file This custom hook is the single source of truth for all analytics data.
 * It encapsulates fetching, loading, error handling, and all data transformation logic.
 * Any component that needs analytics data will use this hook.
 * If the data source or business logic changes, this is the only file to modify.
 */

import { useState, useEffect, useMemo } from 'react';
import { AnalyticsDocument } from '@/lib/types';
import { extractValue } from '@/lib/formatters';
import axios from 'axios';

export const useAnalyticsData = () => {
    const [data, setData] = useState<AnalyticsDocument[]>([]);
    const [isLoading, setIsLoading] = useState<boolean>(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchData = async () => {
            try {
                setIsLoading(true);
                setError(null);
                const response = await axios.get("/analytics-api/analytics");
                console.log('API Response:', response.data);
                const analyticsData = response.data.analytics || [];
                console.log('Analytics Data:', analyticsData);

                setData(analyticsData);
            } catch (err) {
                if (axios.isAxiosError(err)) {
                    setError(err.response?.data?.message || err.message || 'Failed to fetch analytics data');
                } else {
                    setError('An unknown error occurred while fetching data');
                }
                console.error("Failed to fetch analytics data:", err);
            } finally {
                setIsLoading(false);
            }
        };

        fetchData();
    }, []);
    const getDataByType = (type: string): AnalyticsDocument | null => {
        if (!Array.isArray(data)) {
            console.error('Data is not an array:', data);
            return null;
        }
        return data.find(item => item && item.type === type) || null;
    };


    const processingData = useMemo(() => getDataByType('PROCESSING_PERFORMANCE'), [data]);
    const freshnessData = useMemo(() => getDataByType('DATA_FRESHNESS'), [data]);
    const ratingDistData = useMemo(() => getDataByType('RATING_DISTRIBUTION'), [data]);
    const userEngagementData = useMemo(() => getDataByType('USER_ENGAGEMENT'), [data]);
    const userSegmentationData = useMemo(() => getDataByType('USER_SEGMENTATION'), [data]);
    const temporalData = useMemo(() => getDataByType('TEMPORAL_TRENDS'), [data]);
    const moviePopularityData = useMemo(() => getDataByType('MOVIE_POPULARITY'), [data]);
    const genreData = useMemo(() => getDataByType('GENRE_DISTRIBUTION'), [data]);
    const contentPerformanceData = useMemo(() => getDataByType('CONTENT_PERFORMANCE'), [data]);

    const ratingChartData = useMemo(() => {
        if (!ratingDistData) return [];
        return Object.entries(ratingDistData.metrics)
            .filter(([key]) => key.startsWith('rating_'))
            .map(([key, value]) => ({
                rating: key.replace('rating_', '') + '★',
                count: extractValue(value),
                percentage: ((extractValue(value) / extractValue(ratingDistData.metrics.totalRatings)) * 100).toFixed(1)
            }))
            .sort((a, b) => parseFloat(a.rating) - parseFloat(b.rating));
    }, [ratingDistData]);

    const engagementChartData = useMemo(() => {
        if (!userEngagementData) return [];
        const total = extractValue(userEngagementData.metrics.totalEngagedUsers);
        if (total === 0) return [];
        return [
            { name: 'Low Engaged', value: extractValue(userEngagementData.metrics.lowEngagedUsers), color: '#f87171', percentage: ((extractValue(userEngagementData.metrics.lowEngagedUsers) / total) * 100).toFixed(1) },
            { name: 'Moderately Engaged', value: extractValue(userEngagementData.metrics.moderatelyEngagedUsers), color: '#fbbf24', percentage: ((extractValue(userEngagementData.metrics.moderatelyEngagedUsers) / total) * 100).toFixed(1) },
            { name: 'Highly Engaged', value: extractValue(userEngagementData.metrics.highlyEngagedUsers), color: '#34d399', percentage: ((extractValue(userEngagementData.metrics.highlyEngagedUsers) / total) * 100).toFixed(1) }
        ];
    }, [userEngagementData]);

    const segmentationChartData = useMemo(() => {
        if (!userSegmentationData) return [];
        return [
            { activity: 'Low', critical: extractValue(userSegmentationData.metrics.segment_Low_Critical), neutral: extractValue(userSegmentationData.metrics.segment_Low_Neutral), positive: extractValue(userSegmentationData.metrics.segment_Low_Positive) },
            { activity: 'Medium', critical: extractValue(userSegmentationData.metrics.segment_Medium_Critical), neutral: extractValue(userSegmentationData.metrics.segment_Medium_Neutral), positive: extractValue(userSegmentationData.metrics.segment_Medium_Positive) },
            { activity: 'High', critical: extractValue(userSegmentationData.metrics.segment_High_Critical), neutral: extractValue(userSegmentationData.metrics.segment_High_Neutral), positive: extractValue(userSegmentationData.metrics.segment_High_Positive) }
        ];
    }, [userSegmentationData]);

    const topMovies = useMemo(() => {
        if (!moviePopularityData) return [];
        return [
            { title: moviePopularityData.metrics.topMovie_318_title, ratings: extractValue(moviePopularityData.metrics.topMovie_318_ratingsCount), avgRating: moviePopularityData.metrics.topMovie_318_avgRating, genres: moviePopularityData.metrics.topMovie_318_genres },
            { title: moviePopularityData.metrics.topMovie_356_title, ratings: extractValue(moviePopularityData.metrics.topMovie_356_ratingsCount), avgRating: moviePopularityData.metrics.topMovie_356_avgRating, genres: moviePopularityData.metrics.topMovie_356_genres },
            { title: moviePopularityData.metrics.topMovie_296_title, ratings: extractValue(moviePopularityData.metrics.topMovie_296_ratingsCount), avgRating: moviePopularityData.metrics.topMovie_296_avgRating, genres: moviePopularityData.metrics.topMovie_296_genres },
            { title: moviePopularityData.metrics.topMovie_2571_title, ratings: extractValue(moviePopularityData.metrics.topMovie_2571_ratingsCount), avgRating: moviePopularityData.metrics.topMovie_2571_avgRating, genres: moviePopularityData.metrics.topMovie_2571_genres },
            { title: moviePopularityData.metrics.topMovie_593_title, ratings: extractValue(moviePopularityData.metrics.topMovie_593_ratingsCount), avgRating: moviePopularityData.metrics.topMovie_593_avgRating, genres: moviePopularityData.metrics.topMovie_593_genres }
        ].filter(movie => movie.title && movie.title !== 'N/A');
    }, [moviePopularityData]);

    const genreCountData = useMemo(() => {
        if (!genreData) return [];
        return Object.entries(genreData.metrics)
            .filter(([key]) => key.endsWith('_count'))
            .map(([key, value]) => {
                const genreName = key.replace('genre_', '').replace('_count', '');
                return {
                    genre: genreData.metrics[`genre_${genreName}_name`] || genreName.charAt(0).toUpperCase() + genreName.slice(1),
                    count: extractValue(value),
                    avgRating: genreData.metrics[`genre_${genreName}_avgRating`] || 0,
                    uniqueUsers: extractValue(genreData.metrics[`genre_${genreName}_uniqueUsers`] || 0)
                };
            })
            .sort((a, b) => b.count - a.count)
            .slice(0, 12);
    }, [genreData]);

    const genreRatingData = useMemo(() => {
        if (!genreData) return [];
        return Object.entries(genreData.metrics)
            .filter(([key]) => key.endsWith('_avgRating'))
            .map(([key, value]) => {
                const genreName = key.replace('genre_', '').replace('_avgRating', '');
                return {
                    genre: genreData.metrics[`genre_${genreName}_name`] || genreName.charAt(0).toUpperCase() + genreName.slice(1),
                    avgRating: typeof value === 'number' ? value : 0,
                    count: extractValue(genreData.metrics[`genre_${genreName}_count`] || 0)
                };
            })
            .filter(item => item.count > 50000)
            .sort((a, b) => b.avgRating - a.avgRating)
            .slice(0, 10);
    }, [genreData]);

    const monthlyChartData = useMemo(() => {
        if (!temporalData) return [];
        const monthNames = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const monthOrder = monthNames.slice(1);
        return Object.entries(temporalData.metrics)
            .filter(([key]) => key.startsWith('month_') && key.endsWith('_count'))
            .map(([key, value]) => {
                const monthNum = key.split('_')[1];
                return {
                    month: monthNames[parseInt(monthNum)],
                    count: extractValue(value),
                    avgRating: temporalData.metrics[`month_${monthNum}_avgRating`] || 0
                };
            })
            .sort((a, b) => monthOrder.indexOf(a.month) - monthOrder.indexOf(b.month));
    }, [temporalData]);

    const yearlyChartData = useMemo(() => {
        if (!temporalData) return [];
        return Object.entries(temporalData.metrics)
            .filter(([key]) => key.startsWith('year_') && key.endsWith('_count'))
            .map(([key, value]) => {
                const year = key.split('_')[1];
                return {
                    year: parseInt(year),
                    count: extractValue(value),
                    avgRating: temporalData.metrics[`year_${year}_avgRating`] || 0,
                    activeUsers: extractValue(temporalData.metrics[`year_${year}_activeUsers`] || 0)
                };
            })
            .sort((a, b) => a.year - b.year)
            .filter(item => item.year >= 1995 && item.year <= 2023);
    }, [temporalData]);

    const weeklyActivityData = useMemo(() => {
        if (!temporalData) return [];
        const dayNames = ['', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
        const dayOrder = dayNames.slice(1);
        return Object.entries(temporalData.metrics)
            .filter(([key]) => key.startsWith('dayOfWeek_') && key.endsWith('_count'))
            .map(([key, value]) => {
                const dayNum = key.split('_')[1];
                return {
                    day: dayNames[parseInt(dayNum)],
                    count: extractValue(value),
                    avgRating: temporalData.metrics[`dayOfWeek_${dayNum}_avgRating`] || 0
                };
            })
            .sort((a, b) => dayOrder.indexOf(a.day) - dayOrder.indexOf(b.day));
    }, [temporalData]);
    return {
        isLoading,
        error,
        processingData,
        freshnessData,
        ratingDistData,
        userEngagementData,
        userSegmentationData,
        temporalData,
        moviePopularityData,
        genreData,
        contentPerformanceData,
        ratingChartData,
        engagementChartData,
        segmentationChartData,
        topMovies,
        genreCountData,
        genreRatingData,
        monthlyChartData,
        yearlyChartData,
        weeklyActivityData,
    };
};