import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { AnalyticsDocument} from '@/lib/types';
import {Clock} from "lucide-react";
import React from "react";

export const DataFreshnessCard: React.FC<{ data: AnalyticsDocument | null }> = ({ data }) => {
    if (!data) return null;
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader>
                <CardTitle className="flex items-center gap-2 text-emerald-600"><Clock className="h-5 w-5" />Data Freshness</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
                <div className="text-center">
                    <p className="text-sm text-slate-600">Data Age</p>
                    <p className="text-4xl font-bold text-emerald-600">{data.metrics.dataAgeDays}</p>
                    <p className="text-sm text-emerald-500">days old</p>
                </div>
                <div className="pt-2">
                    <p className="text-xs text-slate-500 text-center">Freshness Score: {data.metrics.freshnessScore}/100</p>
                </div>
            </CardContent>
        </Card>
    );
};