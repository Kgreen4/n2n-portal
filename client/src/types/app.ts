export type AppEnvironment = 'development' | 'production';

export type DatabaseConfig = {
  dsn: string;
  maxConns: number;
  maxIdleTime: number;
};

export type GenkitLimitConfig = {
  model: string;
  slots: number;
  pace: number;
  burst: number;
};

export type GenkitConfig = {
  temperature: number;
  gcp_project: string;
  project: string;
  location: string;
  generate: GenkitLimitConfig;
  embed: GenkitLimitConfig;
};

export type AppConfig = {
  environment: AppEnvironment;
  database: DatabaseConfig;
  genkit: GenkitConfig;
};

export type UseAppConfigOptions = {
  environment?: AppEnvironment;
  path?: string;
};
