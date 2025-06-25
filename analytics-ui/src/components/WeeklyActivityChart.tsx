import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer} from 'recharts';
import { Activity } from 'lucide-react';
import { WeeklyActivityData } from '@/lib/types';
import {  formatNumber } from '@/lib/formatters';
export const WeeklyActivityChart: React.FC<{ data: WeeklyActivityData[] }> = ({ data }) => {
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300 lg:col-span-2">
            <CardHeader><CardTitle className="flex items-center gap-2 text-indigo-600"><Activity className="h-5 w-5" />Weekly Activity Patterns</CardTitle></CardHeader>
            <CardContent>
                <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={data}  margin={{top: 10, right: 30, left: 30, bottom: 0}}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
                        <XAxis dataKey="day" stroke="#64748b" />
                        <YAxis stroke="#64748b" />
                        <Tooltip formatter={(value) => [formatNumber(value as number), 'Ratings']} contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }}/>
                        <Bar dataKey="count" fill="#6366f1" />
                    </BarChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
};