import React from 'react';
import { Card, CardContent, CardHeader, CardTitle } from  '@/components/ui/card';
import { Star,Award} from 'lucide-react';
import {formatNumber } from '@/lib/formatters';
export const TopRatedMoviesCard: React.FC<{ data: any[] }> = ({ data }) => {
    return (
        <Card className="bg-white/80 backdrop-blur-sm border-slate-300">
            <CardHeader><CardTitle className="flex items-center gap-2 text-yellow-600"><Award className="h-5 w-5" />Top Rated Movies</CardTitle></CardHeader>
            <CardContent>
                <div className="space-y-4">
                    {data.map((movie, index) => (
                        <div key={index} className="p-4 bg-slate-50 rounded-lg border border-slate-200">
                            <div className="flex items-start justify-between">
                                <div className="flex-1">
                                    <h4 className="font-semibold text-sm text-slate-800">{movie.title}</h4>
                                    <p className="text-xs text-slate-600 mt-1">{movie.genres}</p>
                                    <div className="flex items-center gap-4 text-xs mt-2">
                                        <span>{formatNumber(movie.ratings)} ratings</span>
                                        <span className="flex items-center gap-1 text-yellow-600"><Star className="h-3 w-3 fill-current" />{movie.avgRating.toFixed(2)}</span>
                                    </div>
                                </div>
                                <div className="text-lg font-bold text-yellow-600">#{index + 1}</div>
                            </div>
                        </div>
                    ))}
                </div>
            </CardContent>
        </Card>
    );
};