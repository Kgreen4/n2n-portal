import { run as runPrompt } from '$lib/google/genkit';
import type { AgentComponent, AgentRunInput } from '../../../types/agent';
import { explodeDocumentsByPage, inputForDocumentPage } from './document-pages';
import digitalCheckPrompt from './prompts/digital-check.md?raw';
import patientPrompt from './prompts/patient.md?raw';
import physicalCheckPrompt from './prompts/physical-check.md?raw';

const parsePageOutput = (output: unknown): unknown[] => {
  if (Array.isArray(output)) {
    return output;
  }

  if (typeof output !== 'string') {
    return [];
  }

  try {
    const parsed = JSON.parse(output);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

export const blueCrossBlueShieldPatientAgent = {
  name: 'blue-cross-blue-shield',
  prompt: patientPrompt,
  promptName: 'patient.md',
  execute: async (input, signal) => {
    const pages = await explodeDocumentsByPage(input);
    const pageOutputs = await Promise.all(
      pages.map(async (page) => {
        const output = await runPrompt<unknown, AgentRunInput>({ signal }, patientPrompt, inputForDocumentPage(input, page));
        return parsePageOutput(output);
      })
    );

    return pageOutputs.flat();
  }
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
