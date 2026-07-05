import postgres, { type Sql } from 'postgres';
import { useAppConfig } from '$config';
import type { AppEnvironment } from '../../types/app';

export type PostgresConfig = {
  dsn?: string;
  maxConns?: number;
  maxIdleTime?: number;
  environment?: AppEnvironment;
  configPath?: string;
};

let sqlConnection: Sql | undefined;

export const usePostgresClient = (config: PostgresConfig = {}): Sql => {
  if (sqlConnection) {
    return sqlConnection;
  }

  const appConfig = useAppConfig({
    environment: config.environment,
    path: config.configPath
  });
  const dsn = config.dsn ?? appConfig.database.dsn;

  if (!dsn) {
    throw new Error('postgres: database.dsn is required');
  }

  sqlConnection = postgres(dsn, {
    max: config.maxConns ?? appConfig.database.maxConns,
    idle_timeout: config.maxIdleTime ?? appConfig.database.maxIdleTime
  });

  return sqlConnection;
};

export const createPostgresClient = usePostgresClient;
