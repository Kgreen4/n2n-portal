import { useMemory } from '$hooks/memory/use-memory';
import { usePostgresCollection } from '$lib/postgres';
import type { AgentEngineContext, AgentEngineResult, AgentPromptResult } from '../../../types/agent';
import type {
  Check,
  ClaimLine,
  ClaimTotal,
  EngineOutput,
  MemoryClaim,
  MemoryPatient,
  MemoryRecord,
  PatientClaim
} from './types';

const emptyTotal = (): ClaimTotal => ({
  billed: 0,
  allowed: 0,
  disallowed: 0,
  other: 0,
  copay: 0,
  deductible: 0,
  coinsurance: 0,
  discount: 0,
  interest: 0,
  paid: 0,
  responsibility: 0
});

const asRecord = (value: unknown): Record<string, unknown> => {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
};

const parseJsonOutput = (output: unknown): unknown => {
  if (typeof output !== 'string') {
    return output;
  }

  try {
    return JSON.parse(output);
  } catch {
    return output;
  }
};

const asString = (value: unknown): string | null => {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed || null;
};

const asNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string') {
    const amount = Number(value.replace(/[$,]/g, '').trim());
    return Number.isFinite(amount) ? amount : null;
  }

  return null;
};

const asDate = (value: unknown): string | null => {
  const text = asString(value);
  if (!text) {
    return null;
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString().slice(0, 10);
};

const asCodes = (value: unknown): string[] => {
  if (Array.isArray(value)) {
    return value.map(asString).filter(Boolean) as string[];
  }

  const text = asString(value);
  return text ? text.split(/\s+/).filter(Boolean) : [];
};

const amount = (value: unknown): number => {
  return asNumber(value) ?? 0;
};

const promptOutput = (prompts: AgentPromptResult[], promptName: string): unknown => {
  return parseJsonOutput(prompts.find((prompt) => prompt.prompt === promptName && prompt.status === 'complete')?.output);
};

const normalizeLine = (value: unknown): ClaimLine => {
  const line = asRecord(value);

  return {
    date: asDate(line.date),
    code: asString(line.code),
    service: asString(line.service),
    billed: asNumber(line.billed),
    allowed: asNumber(line.allowed),
    disallowed: asNumber(line.disallowed),
    other: asNumber(line.other),
    copay: asNumber(line.copay),
    deductible: asNumber(line.deductible),
    coinsurance: asNumber(line.coinsurance),
    discount: asNumber(line.discount),
    interest: asNumber(line.interest),
    paid: asNumber(line.paid),
    responsibility: asNumber(line.responsibility),
    codes: asCodes(line.codes)
  };
};

const totalFromLines = (lines: ClaimLine[]): ClaimTotal => {
  return lines.reduce((total, line) => {
    total.billed += amount(line.billed);
    total.allowed += amount(line.allowed);
    total.disallowed += amount(line.disallowed);
    total.other += amount(line.other);
    total.copay += amount(line.copay);
    total.deductible += amount(line.deductible);
    total.coinsurance += amount(line.coinsurance);
    total.discount += amount(line.discount);
    total.interest += amount(line.interest);
    total.paid += amount(line.paid);
    total.responsibility += amount(line.responsibility);
    return total;
  }, emptyTotal());
};

const normalizeTotal = (value: unknown, lines: ClaimLine[]): ClaimTotal => {
  const total = asRecord(value);

  if (!Object.keys(total).length) {
    return totalFromLines(lines);
  }

  return {
    billed: amount(total.billed),
    allowed: amount(total.allowed),
    disallowed: amount(total.disallowed),
    other: amount(total.other),
    copay: amount(total.copay),
    deductible: amount(total.deductible),
    coinsurance: amount(total.coinsurance),
    discount: amount(total.discount),
    interest: amount(total.interest),
    paid: amount(total.paid),
    responsibility: amount(total.responsibility)
  };
};

const normalizePatientClaim = (value: unknown): PatientClaim => {
  const claim = asRecord(value);
  const member = asRecord(claim.member);
  const claimEnvelope = asRecord(claim.claim);
  const lines = Array.isArray(claim.lines) ? claim.lines.map(normalizeLine) : [];

  return {
    member: {
      name: asString(member.name),
      id: asString(member.id)
    },
    provider: asString(claim.provider),
    claim: {
      id: asString(claimEnvelope.id)
    },
    lines,
    total: normalizeTotal(claim.total, lines)
  };
};

const normalizePatientClaims = (value: unknown): PatientClaim[] => {
  return Array.isArray(value) ? value.map(normalizePatientClaim) : [];
};

const normalizeCheck = (value: unknown): Check | null => {
  const check = asRecord(value);
  if (!Object.keys(check).length) {
    return null;
  }

  const drawer = asRecord(check.drawer);
  const micr = asRecord(check.micr);

  return {
    drawer: {
      name: asString(drawer.name),
      address: asString(drawer.address)
    },
    payee: asString(check.payee),
    date: asDate(check.date),
    amount_numeric: asNumber(check.amount_numeric),
    amount_written: asString(check.amount_written),
    bank_name: asString(check.bank_name),
    memo: asString(check.memo),
    check_number: asString(check.check_number),
    micr: {
      routing_number: asString(micr.routing_number),
      account_number: asString(micr.account_number)
    }
  };
};

const patientIdFromClaim = (claim: PatientClaim): string | null => {
  return claim.member.id || claim.claim.id;
};

const splitName = (name: string | null) => {
  const parts = (name || 'Unknown Patient').trim().split(/\s+/);
  const firstName = parts.shift() || 'Unknown';
  const lastName = parts.join(' ') || 'Patient';

  return {
    firstName: firstName.slice(0, 50),
    lastName: lastName.slice(0, 50)
  };
};

const patientFromClaim = (claim: PatientClaim): MemoryPatient | null => {
  const id = patientIdFromClaim(claim);
  if (!id) {
    return null;
  }

  const name = splitName(claim.member.name);

  return {
    id,
    first_name: name.firstName,
    last_name: name.lastName,
    date_of_birth: null,
    insurance_member_id: claim.member.id
  };
};

const claimFromLine = (patientId: string, claim: PatientClaim, line: ClaimLine): MemoryClaim | null => {
  const claimNumber = claim.claim.id;
  const dateOfService = line.date ? new Date(line.date) : null;

  if (!claimNumber || !dateOfService) {
    return null;
  }

  return {
    claim_number: claimNumber,
    patient_id: patientId,
    provider_name: claim.provider,
    date_of_service: dateOfService,
    cpt_hcpcs_code: line.code,
    service_description: line.service,
    billed_amount: amount(line.billed),
    allowed_amount: amount(line.allowed),
    disallowed_amount: amount(line.disallowed),
    co_pay: amount(line.copay),
    deductible: amount(line.deductible),
    co_insurance: amount(line.coinsurance),
    discount_amount: amount(line.discount),
    paid_amount: amount(line.paid),
    patient_responsibility: amount(line.responsibility),
    explanation_code: line.codes.join(' ') || null
  };
};

const toMemoryRecord = (claim: PatientClaim): MemoryRecord | null => {
  const patient = patientFromClaim(claim);
  if (!patient) {
    return null;
  }

  const claims = claim.lines
    .map((line) => claimFromLine(patient.id, claim, line))
    .filter(Boolean) as MemoryClaim[];

  if (!claims.length) {
    return null;
  }

  return {
    patient,
    claims
  };
};

const buildOutput = ({ input, prompts }: AgentEngineContext): EngineOutput => {
  const patientClaims = normalizePatientClaims(promptOutput(prompts, 'patient.md'));
  const physicalCheck = normalizeCheck(promptOutput(prompts, 'physical-check.md'));
  const digitalCheck = normalizeCheck(promptOutput(prompts, 'digital-check.md'));

  return {
    agent: 'blue-cross-blue-shield',
    batchId: input.batchId,
    documents: input.documents.map((document) => ({
      id: document.id,
      name: document.name,
      type: document.type,
      size: document.size
    })),
    promptCount: prompts.length,
    patientClaims,
    checks: {
      physical: physicalCheck,
      digital: digitalCheck
    },
    memoryRecords: patientClaims.map(toMemoryRecord).filter(Boolean) as MemoryRecord[],
    rejectedPrompts: prompts.filter((prompt) => prompt.status === 'failed')
  };
};

export const runBlueCrossBlueShieldEngine = async (context: AgentEngineContext): Promise<AgentEngineResult> => {
  const output = buildOutput(context);

  if (output.rejectedPrompts.length) {
    return {
      status: 'failed',
      output,
      error: 'blue-cross-blue-shield: one or more extraction prompts failed'
    };
  }

  const memory = useMemory(usePostgresCollection());
  const memoryResult = await memory.rememberBlueCrossBlueShield(output);

  return {
    status: 'complete',
    output,
    memory: memoryResult
  };
};
