/**
 * @file Centralized TypeScript type definitions for the analytics data.
 * This ensures type safety and consistency across the entire application.
 */

export interface AnalyticsDocument {
    analyticsId: string;
    generatedAt: string;
    type: string;
    metrics: Record<string, never>;
    description: string;
}
export interface RatingChartData {
    rating: string;
    count: number;
}

export interface EngagementChartData {
    name: string;
    value: number;
    color: string;
    percentage: string;
}

export interface SegmentationChartData {
    activity: string;
    critical: number;
    neutral: number;
    positive: number;
}

export interface TemporalChartData {
    month: string;
    count: number;
    avgRating: number;
}

export interface YearlyChartData {
    year: number;
    count: number;
    avgRating: number;
    activeUsers: number;
}

export interface GenreChartData {
    genre: string;
    count: number;
    avgRating: number;
}

export interface WeeklyActivityData {
    day: string;
    count: number;
    avgRating: number;
}
