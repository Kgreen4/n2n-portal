import type { AgentDocument, AgentPromptResult } from '../../../../types/agent';
import type { Claim } from '../../../../types/claims';
import type { Patient } from '../../../../types/patient';

export type Member = {
  name: string | null;
  id: string | null;
};

export type ClaimEnvelope = {
  id: string | null;
};

export type ClaimLine = {
  date: string | null;
  code: string | null;
  service: string | null;
  billed: number | null;
  allowed: number | null;
  disallowed: number | null;
  other: number | null;
  copay: number | null;
  deductible: number | null;
  coinsurance: number | null;
  discount: number | null;
  interest: number | null;
  paid: number | null;
  responsibility: number | null;
  codes: string[];
};

export type ClaimTotal = {
  billed: number;
  allowed: number;
  disallowed: number;
  other: number;
  copay: number;
  deductible: number;
  coinsurance: number;
  discount: number;
  interest: number;
  paid: number;
  responsibility: number;
};

export type PatientClaim = {
  member: Member;
  provider: string | null;
  claim: ClaimEnvelope;
  lines: ClaimLine[];
  total: ClaimTotal;
};

export type Check = {
  drawer: {
    name: string | null;
    address: string | null;
  };
  payee: string | null;
  date: string | null;
  amount_numeric: number | null;
  amount_written: string | null;
  bank_name: string | null;
  memo: string | null;
  check_number: string | null;
  micr: {
    routing_number: string | null;
    account_number: string | null;
  };
};

export type MemoryClaim = Omit<Claim, 'id' | 'created_at'> & {
  id?: number;
  created_at?: Date;
};

export type MemoryPatient = Omit<Patient, 'created_at'> & {
  created_at?: Date;
};

export type MemoryRecord = {
  patient: MemoryPatient;
  claims: MemoryClaim[];
};

export type MemoryResult = {
  patients: Patient[];
  claims: Claim[];
  skipped: number;
};

export type EngineOutput = {
  agent: 'blue-cross-blue-shield';
  batchId: string;
  documents: Pick<AgentDocument, 'id' | 'name' | 'type' | 'size'>[];
  promptCount: number;
  patientClaims: PatientClaim[];
  checks: {
    physical: Check | null;
    digital: Check | null;
  };
  memoryRecords: MemoryRecord[];
  rejectedPrompts: AgentPromptResult[];
};
