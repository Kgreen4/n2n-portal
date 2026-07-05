import type { AgentRunInput, AgentRunResult, AgentRunStatus } from '../../types/agent';

export type UseAgentState = {
  status: AgentRunStatus;
  result: AgentRunResult | null;
  error: string | null;
};

export const useAgent = () => {
  let status = $state<AgentRunStatus>('idle');
  let result = $state<AgentRunResult | null>(null);
  let error = $state<string | null>(null);

  const execute = (input: AgentRunInput) => {
    status = 'running';
    result = null;
    error = null;

    void fetch('/api/agent', {
      method: 'POST',
      headers: {
        'content-type': 'application/json'
      },
      body: JSON.stringify(input)
    })
      .then(async (response) => {
        const payload = (await response.json()) as AgentRunResult | { error?: string };

        if (!response.ok) {
          throw new Error('error' in payload && payload.error ? payload.error : 'agent: run failed');
        }

        result = payload as AgentRunResult;
        status = result.status;
      })
      .catch((cause) => {
        error = cause instanceof Error ? cause.message : String(cause);
        status = 'failed';
      });
  };

  return {
    get status() {
      return status;
    },
    get result() {
      return result;
    },
    get error() {
      return error;
    },
    execute
  };
};
