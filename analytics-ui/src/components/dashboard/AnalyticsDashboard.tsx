"use client";

/**
 * @file This is the main dashboard part.
 * It's the orchestrator that brings everything together.
 * 1. It calls the `useAnalyticsData` hook to get all data and states.
 * 2. It handles loading and error UI states.
 * 3. It lays out the dashboard structure using Tabs and Sidebar.
 * 4. It passes the processed data down to the individual chart components.
 */

import React, { useState } from 'react';
import { Tabs, TabsContent } from '@/components/ui/tabs';
import { useAnalyticsData } from '@/hooks/useAnalyticsData';
import { DashboardHeader } from './DashboardHeader';
import Sidebar from '@/components/sidebar/Sidebar';
import {ProcessingPerformanceCard} from "@/components/ProcessingPerformanceCard";
import {DataFreshnessCard} from "@/components/DataFreshnessCard";
import {RatingDistributionChart} from "@/components/RatingDistributionChart";
import {RatingQualityCard} from "@/components/RatingQualityCard";
import {UserEngagementChart} from "@/components/UserEngagementChart";
import {UserSegmentationChart} from "@/components/UserSegmentationChart";
import {TopRatedMoviesCard} from "@/components/TopRatedMoviesCard";
import {GenrePopularityChart} from "@/components/GenrePopularityChart";
import {GenreQualityChart} from "@/components/GenreQualityChart";
import {ContentPerformanceCard} from "@/components/ContentPerformanceCard";
import {MonthlyActivityChart} from "@/components/MonthlyActivityChart";
import {YearlyEvolutionChart} from "@/components/YearlyEvolutionChart";
import {WeeklyActivityChart} from "@/components/WeeklyActivityChart";



const AnalyticsDashboard: React.FC = () => {
    // State for active tab
    const [activeTab, setActiveTab] = useState<string>("overview");

    // Call the single hook to get all data and state.
    const {
        isLoading,
        error,
        processingData,
        freshnessData,
        ratingDistData,
        userEngagementData,
        userSegmentationData,
        moviePopularityData,
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
    } = useAnalyticsData();

    // Handle tab changes
    const handleTabChange = (tab: string) => {
        setActiveTab(tab);
    };

    // Handle loading state
    if (isLoading) {
        // This is a simple loader. In Next.js, a `loading.tsx` file provides a better UX.
        return <div className="flex items-center justify-center min-h-screen">Loading analytics data...</div>;
    }

    // Handle error state
    if (error) {
        return <div className="flex items-center justify-center min-h-screen text-red-500">Error: {error}</div>;
    }

    return (
        <div className="flex min-h-screen bg-gradient-to-br from-slate-50 via-slate-100 to-slate-200 text-slate-800">
            {/* Sidebar */}
            <Sidebar activeTab={activeTab} onTabChange={handleTabChange} />

            {/* Main Content */}
            <div className="flex-1 overflow-auto">
                <div className="container mx-auto p-6">
                    <DashboardHeader
                        processingData={processingData}
                        userEngagementData={userEngagementData}
                        moviePopularityData={moviePopularityData}
                    />

                    <Tabs value={activeTab} className="w-full">
                        {/* System Performance Tab */}
                        <TabsContent value="overview" className="space-y-6">
                            <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
                                <ProcessingPerformanceCard data={processingData} />
                                <DataFreshnessCard data={freshnessData} />
                                <RatingQualityCard data={ratingDistData} />
                            </div>
                            <RatingDistributionChart data={ratingChartData} />
                        </TabsContent>

                        {/* User Analytics Tab */}
                        <TabsContent value="behavior" className="space-y-6">
                            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                                <UserEngagementChart data={engagementChartData} rawData={userEngagementData}/>
                                <UserSegmentationChart data={segmentationChartData} rawData={userSegmentationData}/>
                            </div>
                        </TabsContent>

                        {/* Content Insights Tab */}
                        <TabsContent value="content" className="space-y-6">
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                <TopRatedMoviesCard data={topMovies} />
                                <GenrePopularityChart data={genreCountData} />
                            </div>
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                <GenreQualityChart data={genreRatingData} />
                                <ContentPerformanceCard data={contentPerformanceData} />
                            </div>
                        </TabsContent>

                        {/* Temporal Trends Tab */}
                        <TabsContent value="temporal" className="space-y-6">
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                                <MonthlyActivityChart data={monthlyChartData} />
                                <YearlyEvolutionChart data={yearlyChartData} />
                            </div>
                            <WeeklyActivityChart data={weeklyActivityData} />
                        </TabsContent>
                    </Tabs>
                </div>
            </div>
        </div>
    );
};

export default AnalyticsDashboard;
