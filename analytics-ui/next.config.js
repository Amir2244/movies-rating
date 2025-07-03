/** @type {import('next').NextConfig} */
const nextConfig = {
    async rewrites() {
        return [
            {
                source: '/analytics-api/:path*',
                destination: `${process.env.ANALYTICS_API_URL || 'http://localhost:8083'}/analytics-api/:path*`,
            },
        ];
    },
    allowedDevOrigins: ['10.2.0.2'],
    experimental: {
        serverActions: true,
    },
    eslint: {
        // Warning: This allows production builds to successfully complete even if
        // your project has ESLint errors.
        ignoreDuringBuilds: true,
    },
    output: 'standalone',
}

module.exports = nextConfig; 
