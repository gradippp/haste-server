/**
 * Minimal leveled logger.
 *
 * The old server used winston purely for console output with a level filter;
 * that's all this reproduces. Level comes from `LOGGING_LEVEL` (the same env
 * var the previous docker-entrypoint used) and defaults to `info`.
 */

const LEVELS = ["error", "warn", "info", "verbose", "debug"] as const;

export type LogLevel = (typeof LEVELS)[number];

function currentLevel(): number {
  const configured = (process.env.LOGGING_LEVEL || "info").toLowerCase();
  const index = LEVELS.indexOf(configured as LogLevel);
  return index === -1 ? LEVELS.indexOf("info") : index;
}

function emit(level: LogLevel, message: string, meta?: unknown) {
  if (LEVELS.indexOf(level) > currentLevel()) return;

  const line = `[${level}] ${message}`;
  const write =
    level === "error"
      ? console.error
      : level === "warn"
        ? console.warn
        : console.log;

  if (meta === undefined) write(line);
  else write(line, meta);
}

export const logger = {
  error: (message: string, meta?: unknown) => emit("error", message, meta),
  warn: (message: string, meta?: unknown) => emit("warn", message, meta),
  info: (message: string, meta?: unknown) => emit("info", message, meta),
  verbose: (message: string, meta?: unknown) => emit("verbose", message, meta),
  debug: (message: string, meta?: unknown) => emit("debug", message, meta),
};
