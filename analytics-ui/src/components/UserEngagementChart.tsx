import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import { Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend} from 'recharts';
import {Users} from 'lucide-react';
import { AnalyticsDocument, EngagementChartData } from '@/lib/types';
import { formatNumber } from '@/lib/formatters';
export const UserEngagementChart: React.FC<{ data: EngagementChartData[], rawData: AnalyticsDocument | null }> = ({ data, rawData }) => {
    if (!rawData) return null;
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader><CardTitle className="flex items-center gap-2 text-purple-600"><Users className="h-5 w-5" />User Engagement</CardTitle></CardHeader>
            <CardContent>
                <div className="text-center mb-6">
                    <p className="text-sm text-slate-600">Total Engaged Users</p>
                    <p className="text-2xl font-bold text-purple-600">{formatNumber(rawData.metrics.totalEngagedUsers)}</p>
                </div>
                <ResponsiveContainer width="100%" height={250}>
                    <PieChart>
                        <Pie data={data} cx="50%" cy="50%" innerRadius={60} outerRadius={100} paddingAngle={5} dataKey="value">
                            {data.map((entry, index) => <Cell key={`cell-${index}`} fill={entry.color} />)}
                        </Pie>
                        <Tooltip formatter={(value, name, props) => [`${formatNumber(value as number)} (${props.payload.percentage}%)`, 'Users']} contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }}/>
                        <Legend />
                    </PieChart>
                </ResponsiveContainer>
            </CardContent>
        </Card>
    );
};