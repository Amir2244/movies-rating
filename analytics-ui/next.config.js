/** @type {import('next').NextConfig} */
const nextConfig = {
    async rewrites() {
        return [
            {
                source: '/analytics-api/:path*',
                destination: 'http://localhost:8083/analytics-api/:path*',
            },
        ];
    },
    allowedDevOrigins: ['10.2.0.2'],
    experimental: {
        serverActions: true,
    },
}

module.exports = nextConfig; 