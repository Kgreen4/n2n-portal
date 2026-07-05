import { usePostgresCollection } from '$lib/postgres';
import type { ClaimWithPatient } from '../types/claims';
import type { PageServerLoad } from './$types';

const formatDate = (value: string | null) => {
  if (!value) {
    return 'Not provided';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('en-US', {
    day: '2-digit',
    month: 'short',
    year: 'numeric'
  }).format(date);
};

const responsibilityLabel = (claim: ClaimWithPatient) => {
  if (claim.co_pay > 0) {
    return 'Co-pay';
  }

  if (claim.deductible > 0) {
    return 'Deductible';
  }

  if (claim.co_insurance > 0) {
    return 'Co-insurance';
  }

  return claim.patient_responsibility > 0 ? 'Patient balance' : '-';
};

const isFixRequired = (claim: ClaimWithPatient) => {
  const code = claim.explanation_code?.toUpperCase() ?? '';
  return claim.paid_amount === 0 && (code.startsWith('CO-') || code.startsWith('MR'));
};

const daysBetween = (start: string, end: string) => {
  const startDate = new Date(start);
  const endDate = new Date(end);

  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) {
    return null;
  }

  return Math.max(0, Math.round((endDate.getTime() - startDate.getTime()) / 86_400_000));
};

const buildMetrics = (claims: ClaimWithPatient[]) => {
  const turnaroundDays = claims
    .map((claim) => daysBetween(claim.date_of_service, claim.created_at))
    .filter((value): value is number => value !== null);
  const averageClaimTurnaroundDays = turnaroundDays.length
    ? Math.round(turnaroundDays.reduce((total, days) => total + days, 0) / turnaroundDays.length)
    : 0;

  return {
    totalInsurancePaid: claims.reduce((total, claim) => total + claim.paid_amount, 0),
    transferredToPatient: claims.reduce((total, claim) => total + claim.patient_responsibility, 0),
    deniedClaimsValue: claims.reduce((total, claim) => {
      return isFixRequired(claim) ? total + (claim.allowed_amount || claim.billed_amount) : total;
    }, 0),
    averageClaimTurnaroundDays
  };
};

const toLedgerClaim = (claim: ClaimWithPatient) => {
  const fixRequired = isFixRequired(claim);

  return {
    patientName: `${claim.patient_last_name}, ${claim.patient_first_name}`,
    dob: formatDate(claim.patient_date_of_birth),
    payerName: 'BCBS',
    memberId: claim.insurance_member_id ?? claim.patient_id,
    serviceCode: claim.cpt_hcpcs_code ?? '-',
    serviceDesc: claim.service_description ?? 'Not provided',
    allowedAmt: claim.allowed_amount,
    insurancePaid: claim.paid_amount,
    patientOwes: claim.patient_responsibility,
    owesLabel: responsibilityLabel(claim),
    statusClass: fixRequired ? ('denied' as const) : ('clean' as const),
    reasonText: claim.explanation_code ?? 'Processed',
    actionLabel: fixRequired ? 'Fix & Appeal' : 'Post to Chart',
    fixRequired
  };
};

export const load: PageServerLoad = async () => {
  try {
    const claims = await usePostgresCollection().claims.getManyWithPatients();

    return {
      claims: claims.map(toLedgerClaim),
      metrics: buildMetrics(claims),
      claimsError: null
    };
  } catch (error) {
    return {
      claims: [],
      metrics: {
        totalInsurancePaid: 0,
        transferredToPatient: 0,
        deniedClaimsValue: 0,
        averageClaimTurnaroundDays: 0
      },
      claimsError: error instanceof Error ? error.message : String(error)
    };
  }
};
