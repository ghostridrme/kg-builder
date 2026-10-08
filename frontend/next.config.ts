import type { NextConfig } from "next";
const config: NextConfig = {
  agentRules: false,
  distDir: process.env.KG_NEXT_DIST_DIR || ".next",
  async rewrites() {
    return [
      {
        source: "/api/v1/:path*",
        destination: `${process.env.KG_API_URL || "http://127.0.0.1:8080"}/api/v1/:path*`,
      },
    ];
  },
};
export default config;
