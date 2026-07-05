import { run as runPrompt } from '$lib/google/genkit';
import { blueCrossBlueShieldAgents } from './blue-cross-blue-shield/agent';
import { runBlueCrossBlueShieldEngine } from './blue-cross-blue-shield/engine';
import type { AgentComponent, AgentPromptResult, AgentRunInput, AgentRunResult } from '../../types/agent';

const defaultAgents = blueCrossBlueShieldAgents;

export const runAgentComponent = async <Input, Output = unknown>(
  component: AgentComponent<Input, Output>,
  input: AgentRunInput,
  signal?: AbortSignal
): Promise<AgentPromptResult> => {
  if (component.ignore || component.ready?.(input) === false) {
    return {
      status: 'complete',
      agent: component.name,
      prompt: component.promptName,
      output: {
        skipped: true
      }
    };
  }

  try {
    const source = component.source ? await component.source(input) : (input as Input);
    const output = component.execute
      ? await component.execute(source, signal)
      : await runPrompt<Output, Input>({ signal }, component.prompt, source);

    return {
      status: 'complete',
      agent: component.name,
      prompt: component.promptName,
      output
    };
  } catch (error) {
    return {
      status: 'failed',
      agent: component.name,
      prompt: component.promptName,
      error: error instanceof Error ? error.message : String(error)
    };
  }
};

export const runAgent = async (
  input: AgentRunInput,
  signal?: AbortSignal,
  agents: AgentComponent[] = defaultAgents
): Promise<AgentRunResult> => {
  const startedAt = new Date();
  const prompts = await Promise.all(agents.map((agent) => runAgentComponent(agent, input, signal)));
  const engine = await runBlueCrossBlueShieldEngine({
    input,
    prompts,
    signal
  });

  return {
    id: input.batchId,
    status: prompts.some((prompt) => prompt.status === 'failed') || engine.status === 'failed' ? 'failed' : 'complete',
    startedAt,
    completedAt: new Date(),
    prompts,
    engine
  };
};
