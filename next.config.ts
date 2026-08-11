import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // A standalone build emits a self-contained server bundle so the Docker
  // runtime stage can ship without node_modules. It's opt-in because its
  // file tracing step creates symlinks, which plain Windows accounts can't
  // do — the Dockerfile sets this, local builds don't need it.
  output:
    process.env.NEXT_OUTPUT_STANDALONE === "true" ? "standalone" : undefined,

  // The optional storage drivers are `await import()`ed by name in
  // src/lib/stores/index.ts. Only the configured one is ever loaded at
  // runtime, but the bundler would otherwise try to resolve all of them at
  // build time and fail on whichever aren't installed.
  serverExternalPackages: [
    "redis",
    "pg",
    "mysql2",
    "mongodb",
    "memcached",
    "rethinkdbdash",
    "@aws-sdk/client-s3",
    "@google-cloud/datastore",
    "winston",
  ],
};

export default nextConfig;
