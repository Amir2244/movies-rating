import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {Eye } from 'lucide-react';
import { AnalyticsDocument } from '@/lib/types';
import {formatNumber } from '@/lib/formatters';
export const ContentPerformanceCard: React.FC<{ data: AnalyticsDocument | null }> = ({ data }) => {
    if (!data) return null;
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300 col-span-1 lg:col-span-2">
            <CardHeader><CardTitle className="flex items-center gap-2 text-emerald-600"><Eye className="h-5 w-5" />Content Performance</CardTitle></CardHeader>
            <CardContent>
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-6 text-center">
                    <div className="p-3 bg-blue-500/10 rounded-lg"><p className="text-sm text-slate-600">Total Movies</p><p className="text-2xl font-bold text-blue-600">{formatNumber(data.metrics.performance_totalMovies)}</p></div>
                    <div className="p-3 bg-green-500/10 rounded-lg"><p className="text-sm text-slate-600">High Rated</p><p className="text-2xl font-bold text-green-600">{formatNumber(data.metrics.performance_highRatedMovies)}</p></div>
                    <div className="p-3 bg-yellow-500/10 rounded-lg"><p className="text-sm text-slate-600">Polarizing</p><p className="text-2xl font-bold text-yellow-600">{formatNumber(data.metrics.performance_polarizingMovies)}</p></div>
                    <div className="p-3 bg-purple-500/10 rounded-lg"><p className="text-sm text-slate-600">Ratings/Movie</p><p className="text-2xl font-bold text-purple-600">{data.metrics.performance_avgRatingsPerMovie?.toFixed(0)}</p></div>
                </div>
            </CardContent>
        </Card>
    );
};