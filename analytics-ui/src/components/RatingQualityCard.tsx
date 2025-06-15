import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {AnalyticsDocument} from "@/lib/types";
import {Star} from "lucide-react";

export const RatingQualityCard: React.FC<{ data: AnalyticsDocument | null }> = ({ data }) => {
    if (!data) return null;
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader>
                <CardTitle className="flex items-center gap-2 text-yellow-600"><Star className="h-5 w-5" />Rating Quality</CardTitle>
            </CardHeader>
            <CardContent>
                <div className="space-y-4">
                    <div className="text-center">
                        <p className="text-3xl font-bold text-yellow-600">{data.metrics.avgRating?.toFixed(2) || 'N/A'}</p>
                        <p className="text-sm text-slate-600">Average Rating</p>
                    </div>
                    <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                            <p className="text-slate-600">Most Common</p>
                            <p className="text-lg font-bold text-yellow-500">{data.metrics.mostCommonRating}★</p>
                        </div>
                        <div>
                            <p className="text-slate-600">Median</p>
                            <p className="text-lg font-bold text-yellow-500">{data.metrics.ratingMedian}★</p>
                        </div>
                    </div>
                </div>
            </CardContent>
        </Card>
    );
};