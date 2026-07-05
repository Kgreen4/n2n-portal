import type { AgentComponent, AgentRunInput } from '../../../types/agent';
import digitalCheckPrompt from './prompts/digital-check.md?raw';
import patientPrompt from './prompts/patient.md?raw';
import physicalCheckPrompt from './prompts/physical-check.md?raw';

export const blueCrossBlueShieldPatientAgent = {
  name: 'blue-cross-blue-shield',
  prompt: patientPrompt,
  promptName: 'patient.md'
} satisfies AgentComponent<AgentRunInput>;

export const blueCrossBlueShieldPhysicalCheckAgent = {
  name: 'blue-cross-blue-shield',
  prompt: physicalCheckPrompt,
  promptName: 'physical-check.md'
} satisfies AgentComponent<AgentRunInput>;

export const blueCrossBlueShieldDigitalCheckAgent = {
  name: 'blue-cross-blue-shield',
  prompt: digitalCheckPrompt,
  promptName: 'digital-check.md'
} satisfies AgentComponent<AgentRunInput>;

export const blueCrossBlueShieldAgents = [
  blueCrossBlueShieldPatientAgent,
  blueCrossBlueShieldPhysicalCheckAgent,
  blueCrossBlueShieldDigitalCheckAgent
] satisfies AgentComponent[];
