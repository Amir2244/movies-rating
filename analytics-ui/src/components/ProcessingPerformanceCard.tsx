
import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import { Gauge} from 'lucide-react';
import { AnalyticsDocument} from '@/lib/types';
import { formatNumber } from "@/lib/formatters"


export const ProcessingPerformanceCard: React.FC<{ data: AnalyticsDocument | null }> = ({ data }) => {
    if (!data) return null;
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300 col-span-1 lg:col-span-2">
            <CardHeader>
                <CardTitle className="flex items-center gap-2 text-blue-600"><Gauge className="h-5 w-5" />Processing Performance</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-6">
                    <div className="space-y-2">
                        <p className="text-sm text-slate-600">Total Records Processed</p>
                        <p className="text-3xl font-bold text-blue-600">{formatNumber(data.metrics.totalRecords)}</p>
                    </div>
                    <div className="space-y-2">
                        <p className="text-sm text-slate-600">Processing Time Test</p>
                        <p className="text-3xl font-bold text-emerald-600">{data.metrics.processingTimeMs}ms</p>
                    </div>

                    <div className="space-y-2">
                        <p className="text-sm text-slate-600">Data Quality</p>
                        <p className="text-3xl font-bold text-green-600">{data.metrics.dataQualityScore}%</p>
                    </div>
                </div>
            </CardContent>
        </Card>
    );
};