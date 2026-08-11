import fs from "fs";
import path from "path";

/**
 * Configuration is `config.json` with environment variables layered on top.
 *
 * The env var names are deliberately identical to the ones the old
 * `docker-entrypoint.js` produced (`STORAGE_TYPE`, `MAX_LENGTH`,
 * `KEYGENERATOR_TYPE`, `RATELIMITS_*`, `DOCUMENTS`, …) so existing
 * deployments keep working. Note that the old entrypoint wrote its output to
 * `config.js` while `server.js` read `config.json` — meaning none of those
 * env vars ever actually took effect. They do now.
 */

export type StorageType =
  | "file"
  | "memory"
  | "redis"
  | "postgres"
  | "mariadb"
  | "mongo"
  | "memcached"
  | "rethinkdb"
  | "amazon-s3"
  | "google-datastore";

export type KeyGeneratorType = "random" | "phonetic" | "dictionary";

export interface StorageConfig {
  type: StorageType;
  host?: string;
  port?: number;
  /** Seconds until a document expires. Unset/0 means never. */
  expire?: number;
  db?: string | number;
  user?: string;
  password?: string;
  path?: string;
  connectionUrl?: string;
  bucket?: string;
  region?: string;
  database?: string;
  table?: string;
  [key: string]: unknown;
}

export interface RateLimitCategory {
  totalRequests: number;
  every: number;
}

export interface RateLimitConfig {
  whitelist: string[];
  blacklist: string[];
  categories: {
    normal?: RateLimitCategory | null;
    whitelist?: RateLimitCategory | null;
    blacklist?: RateLimitCategory | null;
  };
}

export interface Config {
  keyLength: number;
  maxLength: number;
  storage: StorageConfig;
  keyGenerator: { type: KeyGeneratorType; keyspace?: string; path?: string };
  rateLimits: RateLimitConfig | null;
  /** name -> file path, seeded into the store and never expired */
  documents: Record<string, string>;
}

const DEFAULTS = {
  keyLength: 10,
  maxLength: 400000,
} as const;

function num(value: string | undefined): number | undefined {
  if (value === undefined || value.trim() === "") return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function list(value: string | undefined): string[] | undefined {
  if (!value) return undefined;
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function category(
  totalRequests: string | undefined,
  every: string | undefined,
  fallback?: RateLimitCategory | null
): RateLimitCategory | null {
  const total = num(totalRequests) ?? fallback?.totalRequests;
  const window = num(every) ?? fallback?.every;
  if (total === undefined || window === undefined) return null;
  return { totalRequests: total, every: window };
}

function readConfigFile(): Record<string, unknown> {
  const configPath = process.env.CONFIG_PATH
    ? path.resolve(process.env.CONFIG_PATH)
    : path.resolve(process.cwd(), "config.json");

  try {
    return JSON.parse(fs.readFileSync(configPath, "utf8"));
  } catch {
    // A missing or unreadable config.json is fine — env vars and defaults
    // are enough to run.
    return {};
  }
}

export function buildConfig(
  // Typed loosely rather than as NodeJS.ProcessEnv so tests can pass a plain
  // object with just the keys under test.
  env: Record<string, string | undefined> = process.env,
  file: Record<string, unknown> = readConfigFile()
): Config {
  const fileStorage: Partial<StorageConfig> =
    (file.storage as StorageConfig | undefined) ?? {};
  const fileKeyGen =
    (file.keyGenerator as Config["keyGenerator"] | undefined) ?? {
      type: "random",
    };
  const fileRateLimits = file.rateLimits as RateLimitConfig | undefined;

  const storage: StorageConfig = {
    ...fileStorage,
    type: (env.STORAGE_TYPE || fileStorage.type || "file") as StorageType,
    host: env.STORAGE_HOST || fileStorage.host,
    port: num(env.STORAGE_PORT) ?? fileStorage.port,
    expire: num(env.STORAGE_EXPIRE_SECONDS) ?? fileStorage.expire,
    db: env.STORAGE_DB || fileStorage.db,
    user: env.STORAGE_USERNAME || fileStorage.user,
    password: env.STORAGE_PASSWORD || fileStorage.password,
    path: env.STORAGE_FILEPATH || fileStorage.path,
    bucket: env.STORAGE_AWS_BUCKET || fileStorage.bucket,
    region: env.STORAGE_AWS_REGION || fileStorage.region,
  };

  if (env.STORAGE_CONNECTION_URL) {
    storage.connectionUrl = env.STORAGE_CONNECTION_URL;
  }

  const rateLimits: RateLimitConfig | null = (() => {
    const normal = category(
      env.RATELIMITS_NORMAL_TOTAL_REQUESTS,
      env.RATELIMITS_NORMAL_EVERY_MILLISECONDS,
      fileRateLimits?.categories?.normal
    );
    const whitelist = category(
      env.RATELIMITS_WHITELIST_TOTAL_REQUESTS,
      env.RATELIMITS_WHITELIST_EVERY_MILLISECONDS,
      fileRateLimits?.categories?.whitelist
    );
    const blacklist = category(
      env.RATELIMITS_BLACKLIST_TOTAL_REQUESTS,
      env.RATELIMITS_BLACKLIST_EVERY_MILLISECONDS,
      fileRateLimits?.categories?.blacklist
    );

    if (!normal && !whitelist && !blacklist) return null;

    return {
      whitelist: list(env.RATELIMITS_WHITELIST) ?? fileRateLimits?.whitelist ?? [],
      blacklist: list(env.RATELIMITS_BLACKLIST) ?? fileRateLimits?.blacklist ?? [],
      categories: { normal, whitelist, blacklist },
    };
  })();

  const documents: Record<string, string> = (() => {
    if (env.DOCUMENTS) {
      return env.DOCUMENTS.split(",").reduce<Record<string, string>>(
        (acc, item) => {
          const [name, docPath] = item.trim().split("=");
          if (name && docPath) acc[name] = docPath;
          return acc;
        },
        {}
      );
    }
    return (file.documents as Record<string, string> | undefined) ?? {};
  })();

  return {
    keyLength:
      num(env.KEY_LENGTH) ?? (file.keyLength as number) ?? DEFAULTS.keyLength,
    maxLength:
      num(env.MAX_LENGTH) ?? (file.maxLength as number) ?? DEFAULTS.maxLength,
    storage,
    keyGenerator: {
      type: (env.KEYGENERATOR_TYPE ||
        fileKeyGen.type ||
        "random") as KeyGeneratorType,
      keyspace: env.KEYGENERATOR_KEYSPACE || fileKeyGen.keyspace,
      path: env.KEYGENERATOR_PATH || fileKeyGen.path,
    },
    rateLimits,
    documents,
  };
}

let cached: Config | null = null;

/** The process-wide config, built once on first use. */
export function getConfig(): Config {
  if (!cached) cached = buildConfig();
  return cached;
}

/** Test-only: drop the memoized config so the next getConfig() rebuilds. */
export function resetConfig() {
  cached = null;
}
