import type { Sql } from 'postgres';
import { useClaimRepository } from '$hooks/claims/collection/use-claim-repository';
import { usePatientRepository } from '$hooks/patients/collection/use-patient-repository';
import type { Collection } from '../../types/collection';

export const useCollection = (sql: Sql): Collection => {
  return {
    patients: usePatientRepository(sql),
    claims: useClaimRepository(sql)
  };
};
