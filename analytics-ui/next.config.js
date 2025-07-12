/** @type {import('next').NextConfig} */
const nextConfig = {
    async rewrites() {
        return [
            {
                source: '/analytics-api/:path*',
                destination: `${process.env.ANALYTICS_API_URL || 'http://localhost:8083'}/analytics-api/:path*`,
            },
            {
                source: '/recommendations-api/:path*',
                destination: `${process.env.RECOMMENDATIONS_API_URL || 'http://localhost:8082'}/recommendations-api/:path*`,
            },
        ];
    },
    experimental: {
        serverActions: {
            allowedOrigins: process.env.ALLOWED_ORIGINS ? process.env.ALLOWED_ORIGINS.split(',') : [],
        },
    },
    eslint: {
        ignoreDuringBuilds: true,
    },
    output: 'standalone',
}

module.exports = nextConfig;