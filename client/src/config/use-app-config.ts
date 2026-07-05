import { dev } from '$app/environment';
import { existsSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { parse } from 'yaml';
import type { AppConfig, AppEnvironment, UseAppConfigOptions } from '../types/app';

const configCache = new Map<string, AppConfig>();

const currentEnvironment = (): AppEnvironment => {
  return dev ? 'development' : 'production';
};

const configPathFor = (environment: AppEnvironment) => {
  return join(process.cwd(), 'deploy', 'configs', `${environment}.yaml`);
};

const assertRecord = (value: unknown, label: string): Record<string, unknown> => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`app config: ${label} must be an object`);
  }

  return value as Record<string, unknown>;
};

const assertString = (value: unknown, label: string) => {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new Error(`app config: ${label} must be a non-empty string`);
  }

  return value;
};

const assertNumber = (value: unknown, label: string) => {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    throw new Error(`app config: ${label} must be a number`);
  }

  return value;
};

const normalizeConfig = (value: unknown, environment: AppEnvironment): AppConfig => {
  const config = assertRecord(value, 'root');
  const database = assertRecord(config.database, 'database');
  const genkit = assertRecord(config.genkit, 'genkit');
  const generate = assertRecord(genkit.generate, 'genkit.generate');
  const embed = assertRecord(genkit.embed, 'genkit.embed');

  return {
    environment,
    database: {
      dsn: assertString(database.dsn, 'database.dsn'),
      maxConns: assertNumber(database.maxConns ?? 10, 'database.maxConns'),
      maxIdleTime: assertNumber(database.maxIdleTime ?? 20, 'database.maxIdleTime')
    },
    genkit: {
      temperature: assertNumber(genkit.temperature ?? 0, 'genkit.temperature'),
      project: assertString(genkit.project, 'genkit.project'),
      location: assertString(genkit.location, 'genkit.location'),
      generate: {
        model: assertString(generate.model, 'genkit.generate.model'),
        slots: assertNumber(generate.slots ?? 3, 'genkit.generate.slots'),
        pace: assertNumber(generate.pace ?? 1000, 'genkit.generate.pace'),
        burst: assertNumber(generate.burst ?? 3, 'genkit.generate.burst')
      },
      embed: {
        model: assertString(embed.model, 'genkit.embed.model'),
        slots: assertNumber(embed.slots ?? 6, 'genkit.embed.slots'),
        pace: assertNumber(embed.pace ?? 333, 'genkit.embed.pace'),
        burst: assertNumber(embed.burst ?? 6, 'genkit.embed.burst')
      }
    }
  };
};

export const useAppConfig = (options: UseAppConfigOptions = {}): AppConfig => {
  const environment = options.environment ?? currentEnvironment();
  const path = resolve(options.path ?? configPathFor(environment));

  if (configCache.has(path)) {
    return configCache.get(path) as AppConfig;
  }

  if (!existsSync(path)) {
    throw new Error(`app config: unable to find ${path}`);
  }

  const parsed = parse(readFileSync(path, 'utf8'));
  const config = normalizeConfig(parsed, environment);
  configCache.set(path, config);

  return config;
};
