import React from 'react';

export default function Loading() {
    return (
        <div className="flex flex-col items-center justify-center min-h-screen bg-slate-100 text-slate-700">
            <div className="w-16 h-16 border-4 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
            <p className="mt-4 text-lg font-semibold">Loading Dashboard...</p>
        </div>
    );
}
