import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer} from 'recharts';
import {  TrendingUp } from 'lucide-react';
import { GenreChartData } from '@/lib/types';
import {formatNumber } from '@/lib/formatters';
export const GenrePopularityChart: React.FC<{ data: GenreChartData[] }> = ({ data }) => {
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader><CardTitle className="flex items-center gap-2 text-blue-600"><TrendingUp className="h-5 w-5" />Genre Popularity by Ratings</CardTitle></CardHeader>
            <CardContent>
                <ResponsiveContainer width="100%" height={400}>
                    <BarChart data={data} layout="vertical">
                        <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
                        <XAxis type="number" stroke="#64748b" />
                        <YAxis dataKey="genre" type="category" stroke="#64748b" width={80} fontSize={12} />
                        <Tooltip formatter={(value) => [formatNumber(value as number), 'Total Ratings']} contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }}/>
                        <Bar dataKey="count" fill="#3b82f6" />
                    </BarChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
};