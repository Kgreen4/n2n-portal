import { json } from '@sveltejs/kit';
import { runAgent } from '$agents/agent';
import { useGoogleGenkit } from '$lib/google/client';
import type { RequestHandler } from './$types';
import type { AgentRunInput } from '../../../types/agent';

const isAgentRunInput = (value: unknown): value is AgentRunInput => {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const input = value as Partial<AgentRunInput>;
  return (
    typeof input.batchId === 'string' &&
    typeof input.notes === 'string' &&
    Array.isArray(input.documents) &&
    input.documents.every((document) => {
      return (
        document &&
        typeof document.id === 'string' &&
        typeof document.name === 'string' &&
        typeof document.type === 'string' &&
        typeof document.extension === 'string' &&
        typeof document.size === 'number' &&
        typeof document.base64 === 'string'
      );
    })
  );
};

export const POST: RequestHandler = async ({ request }) => {
  const input = await request.json();

  if (!isAgentRunInput(input)) {
    return json({ error: 'agent: invalid run input' }, { status: 400 });
  }

  await useGoogleGenkit();
  const result = await runAgent(input, request.signal);
  return json(result);
};
