import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import {  Star} from 'lucide-react';
import { GenreChartData} from '@/lib/types';

export const GenreQualityChart: React.FC<{ data: GenreChartData[] }> = ({ data }) => {
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader><CardTitle className="flex items-center gap-2 text-green-600"><Star className="h-5 w-5" />Highest Rated Genres</CardTitle></CardHeader>
            <CardContent>
                <ResponsiveContainer width="100%" height={350}>
                    <BarChart data={data} layout="vertical">
                        <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
                        <XAxis type="number" domain={[3.0, 'auto']} stroke="#64748b" />
                        <YAxis dataKey="genre" type="category" stroke="#64748b" width={80} fontSize={12} />
                        <Tooltip formatter={(value) => [(value as number).toFixed(2) + '★', 'Average Rating']} contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }}/>
                        <Bar dataKey="avgRating" fill="#10b981" />
                    </BarChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
};