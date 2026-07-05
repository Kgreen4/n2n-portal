import type {
  EngineOutput,
  MemoryRecord,
  MemoryResult
} from '../../features/agents/blue-cross-blue-shield/types';
import { useClaimRecord } from '../claims/use-claim-record';
import { usePatientRecord } from '../patients/use-patient-record';
import type { Collection } from '../../types/collection';

const persistBlueCrossRecord = async (collection: Collection, record: MemoryRecord) => {
  const patients = usePatientRecord(collection.patients);
  const claimRecords = useClaimRecord(collection.claims);
  const patient = await patients.findOrPut(record.patient);
  const persistedClaims = await Promise.all(
    record.claims.map((claim) =>
      claimRecords.putUnique({
        ...claim,
        patient_id: patient.id
      })
    )
  );

  return {
    patient,
    claims: persistedClaims
  };
};

export const useMemory = (collection: Collection) => ({
  rememberBlueCrossBlueShield: async (output: EngineOutput): Promise<MemoryResult> => {
    const persisted = await Promise.all(output.memoryRecords.map((record) => persistBlueCrossRecord(collection, record)));

    return {
      patients: persisted.map((record) => record.patient),
      claims: persisted.flatMap((record) => record.claims),
      skipped: output.patientClaims.length - output.memoryRecords.length
    };
  }
});
