import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer} from 'recharts';
import {  Target} from 'lucide-react';
import { AnalyticsDocument, SegmentationChartData} from '@/lib/types';
export const UserSegmentationChart: React.FC<{ data: SegmentationChartData[], rawData: AnalyticsDocument | null }> = ({ data, rawData }) => {
    if (!rawData) return null;
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300 col-span-1 lg:col-span-2">
            <CardHeader><CardTitle className="flex items-center gap-2 text-pink-600"><Target className="h-5 w-5" />User Segmentation</CardTitle></CardHeader>
            <CardContent>
                <div className="grid grid-cols-3 gap-4 mb-6 text-center">
                    <div className="p-2 bg-green-500/10 rounded-lg"><p className="text-sm text-slate-600">Positive</p><p className="font-bold text-green-600">{rawData.metrics.positiveUsersPercentage?.toFixed(1)}%</p></div>
                    <div className="p-2 bg-yellow-500/10 rounded-lg"><p className="text-sm text-slate-600">Neutral</p><p className="font-bold text-yellow-600">{rawData.metrics.neutralUsersPercentage?.toFixed(1)}%</p></div>
                    <div className="p-2 bg-red-500/10 rounded-lg"><p className="text-sm text-slate-600">Critical</p><p className="font-bold text-red-500">{rawData.metrics.criticalUsersPercentage?.toFixed(1)}%</p></div>
                </div>
                <ResponsiveContainer width="100%" height={250}>
                    <BarChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
                        <XAxis dataKey="activity" stroke="#64748b" />
                        <YAxis stroke="#64748b" />
                        <Tooltip contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }} />
                        <Bar dataKey="positive" stackId="a" fill="#34d399" name="Positive" />
                        <Bar dataKey="neutral" stackId="a" fill="#fbbf24" name="Neutral" />
                        <Bar dataKey="critical" stackId="a" fill="#f87171" name="Critical" />
                    </BarChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
};