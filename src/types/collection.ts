import type { ClaimRepository } from "./claims";
import type { PatientRepository } from "./patient";

export type Collection = {
  patients: PatientRepository;
  claims: ClaimRepository;
};

export type CollectionConfig = {
  dsn: string;
  maxConns?: number;
  maxIdleTime?: number;
};