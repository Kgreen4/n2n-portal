export type AgentName = 'blue-cross-blue-shield';

export type AgentDocument = {
  id: string;
  name: string;
  type: string;
  extension: string;
  size: number;
  base64: string;
};

export type AgentRunInput = {
  batchId: string;
  notes: string;
  documents: AgentDocument[];
};

export type AgentComponent<Input = AgentRunInput, Output = unknown> = {
  name: AgentName;
  prompt: string;
  promptName: string;
  ignore?: boolean;
  ready?: (input: AgentRunInput) => boolean;
  source?: (input: AgentRunInput) => Input | Promise<Input>;
  execute?: (input: Input, signal?: AbortSignal) => Promise<Output>;
};

export type AgentPromptResult = {
  status: 'complete' | 'failed';
  agent: AgentName;
  prompt: string;
  output?: unknown;
  error?: string;
};

export type AgentEngineContext = {
  input: AgentRunInput;
  prompts: AgentPromptResult[];
  signal?: AbortSignal;
};

export type AgentEngineResult = {
  status: 'complete' | 'failed';
  output?: unknown;
  memory?: unknown;
  error?: string;
};

export type AgentRunStatus = 'idle' | 'running' | 'complete' | 'failed';

export type AgentRunResult = {
  id: string;
  status: AgentRunStatus;
  startedAt: Date;
  completedAt?: Date;
  prompts: AgentPromptResult[];
  engine?: AgentEngineResult;
};
