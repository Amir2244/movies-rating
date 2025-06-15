import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,  Line, Area,  ComposedChart } from 'recharts';
import {Calendar } from 'lucide-react';
import {  TemporalChartData } from '@/lib/types';
import { formatNumber } from '@/lib/formatters';
export const MonthlyActivityChart: React.FC<{ data: TemporalChartData[] }> = ({ data }) => {
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader><CardTitle className="flex items-center gap-2 text-emerald-600"><Calendar className="h-5 w-5" />Monthly Activity</CardTitle></CardHeader>
            <CardContent>
                <ResponsiveContainer width="100%" height={350}>
                    <ComposedChart data={data}>
                        <defs><linearGradient id="monthlyGradient" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#10b981" stopOpacity={0.8}/><stop offset="95%" stopColor="#10b981" stopOpacity={0.1}/></linearGradient></defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
                        <XAxis dataKey="month" stroke="#64748b" />
                        <YAxis yAxisId="left" stroke="#10b981" />
                        <YAxis yAxisId="right" orientation="right" stroke="#f59e0b" />
                        <Tooltip contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }} formatter={(value, name) => [name === 'count' ? formatNumber(value as number) : (value as number).toFixed(2) + '★', name === 'count' ? 'Ratings' : 'Avg Rating']}/>
                        <Area yAxisId="left" type="monotone" dataKey="count" stroke="#10b981" fill="url(#monthlyGradient)" strokeWidth={3} />
                        <Line yAxisId="right" type="monotone" dataKey="avgRating" stroke="#f59e0b" strokeWidth={2} />
                    </ComposedChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
};
