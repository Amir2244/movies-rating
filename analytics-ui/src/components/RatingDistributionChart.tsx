import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import {RatingChartData} from "@/lib/types";
import { BarChart, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Area, AreaChart} from 'recharts';
import {formatNumber} from "@/lib/formatters";

export const RatingDistributionChart: React.FC<{ data: RatingChartData[] }> = ({ data }) => {
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
        <CardHeader>
            <CardTitle className="flex items-center gap-2 text-yellow-600"><BarChart className="h-5 w-5" />Detailed Rating Distribution</CardTitle>
    </CardHeader>
    <CardContent>
    <ResponsiveContainer width="100%" height={300}>
    <AreaChart data={data}>
    <defs><linearGradient id="ratingGradient" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="#fbbf24" stopOpacity={0.8}/><stop offset="95%" stopColor="#fbbf24" stopOpacity={0.1}/></linearGradient></defs>
    <CartesianGrid strokeDasharray="3 3" stroke="#cbd5e1" />
    <XAxis dataKey="rating" stroke="#64748b" />
    <YAxis stroke="#64748b" />
    <Tooltip contentStyle={{ backgroundColor: 'rgba(255, 255, 255, 0.95)', border: '1px solid #cbd5e1', borderRadius: '8px' }} formatter={(value) => [formatNumber(value as number), 'Ratings']}/>
    <Area type="monotone" dataKey="count" stroke="#fbbf24" fill="url(#ratingGradient)" strokeWidth={2} />
    </AreaChart>
    </ResponsiveContainer>
    </CardContent>
    </Card>
);
};
