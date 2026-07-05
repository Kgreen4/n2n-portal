export type Patient = {
    id: string;
    first_name: string;
    last_name: string;
    date_of_birth: Date | null;
    insurance_member_id: string | null;
    created_at: Date;
};

export type PatientRepository = {
  getOneByID: (id: string) => Promise<Patient | null>;
  getOneByInsuranceID: (memberId: string) => Promise<Patient | null>;
  put: (patient: Omit<Patient, 'created_at'> & { created_at?: Date }) => Promise<Patient>;
  delete: (id: string) => Promise<boolean>;
};
