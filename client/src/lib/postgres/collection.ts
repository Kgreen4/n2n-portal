import { useCollection } from '$hooks/collection/use-collection';
import type { Collection } from '../../types/collection';
import { usePostgresClient, type PostgresConfig } from './client';

export const usePostgresCollection = (config: PostgresConfig = {}): Collection => {
  return useCollection(usePostgresClient(config));
};
