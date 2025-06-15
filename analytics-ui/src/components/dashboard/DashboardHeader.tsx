
import React from 'react';
import { AnalyticsDocument } from '@/lib/types';
import { formatNumber } from '@/lib/formatters';

interface DashboardHeaderProps {
    processingData: AnalyticsDocument | null;
    userEngagementData: AnalyticsDocument | null;
    moviePopularityData: AnalyticsDocument | null;
}

export const DashboardHeader: React.FC<DashboardHeaderProps> = ({ processingData, userEngagementData, moviePopularityData }) => {
    return (
        <div className="mb-8 text-center">
            <h1 className="text-5xl font-bold mb-4 bg-gradient-to-r from-blue-600 via-purple-600 to-pink-600 bg-clip-text text-transparent">
                MovieLens Analytics Dashboard
            </h1>
            <p className="text-slate-600 text-lg">
                Comprehensive insights into movie recommendation system performance & user behavior
            </p>
            <div className="mt-4 flex justify-center space-x-8 text-sm">
                <div className="text-center">
                    <div className="text-2xl font-bold text-blue-600">
                        {processingData ? formatNumber(processingData.metrics.totalRecords) : '...'}
                    </div>
                    <div className="text-slate-500">Total Records</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-purple-600">
                        {userEngagementData ? formatNumber(userEngagementData.metrics.totalEngagedUsers) : '...'}
                    </div>
                    <div className="text-slate-500">Active Users</div>
                </div>
                <div className="text-center">
                    <div className="text-2xl font-bold text-pink-600">
                        {moviePopularityData ? formatNumber(moviePopularityData.metrics.popularity_totalMovies) : '...'}
                    </div>
                    <div className="text-slate-500">Movies</div>
                </div>
            </div>
        </div>
    );
};
