import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import {Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,  Line, ComposedChart } from 'recharts';
import { TrendingUp } from 'lucide-react';
import {YearlyChartData} from '@/lib/types';
import { formatNumber } from '@/lib/formatters';
export const YearlyEvolutionChart: React.FC<{ data: YearlyChartData[] }> = ({ data }) => {
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader><CardTitle className="flex items-center gap-2 text-blue-600"><TrendingUp className="h-5 w-5" />Platform Evolution</CardTitle></CardHeader>
            <CardContent>
                <ResponsiveContainer width="100%" height={350}>
                    <ComposedChart data={data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
                        <XAxis dataKey="year" stroke="#64748b" />
                        <YAxis yAxisId="left" stroke="#3b82f6" />
                        <YAxis yAxisId="right" orientation="right" stroke="#8b5cf6" />
                        <Tooltip contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }} formatter={(value, name) => { if (name === 'count') return [formatNumber(value as number), 'Ratings']; if (name === 'activeUsers') return [formatNumber(value as number), 'Active Users']; return [(value as number).toFixed(2), 'Avg Rating']; }}/>
                        <Bar yAxisId="left" dataKey="count" fill="#3b82f6" />
                        <Line yAxisId="right" type="monotone" dataKey="activeUsers" stroke="#8b5cf6" strokeWidth={2} />
                    </ComposedChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
};
