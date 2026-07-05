import { genkit, type Genkit } from 'genkit';
import { vertexAI } from '@genkit-ai/google-genai';

export type Model = string;

export const Lite: Model = 'vertexai/gemini-2.5-flash-lite';
export const Medium: Model = 'vertexai/gemini-2.5-flash';
export const High: Model = 'vertexai/gemini-1.5-pro';

export type Limit = {
  model: string;
  slots: number;
  pace: number;
  burst: number;
};

export type Config = {
  temperature: number;
  project: string;
  location: string;
  generate: Limit;
  embed: Limit;
};

export type PromptTemplate<Input = unknown> =
  | string
  | ((input: Input) => string)
  | {
      render: (input: Input) => string;
    };

type RuntimeContext = AbortSignal | { signal?: AbortSignal } | undefined;

type Deferred = {
  resolve: () => void;
  reject: (error: unknown) => void;
};

type Throttle = {
  acquire: (signal?: AbortSignal) => Promise<void>;
  release: () => void;
  postpone: (wait: number) => void;
};

let engine: Genkit | undefined;
let model = '';
let embedder = '';
let temperature = 0;

const useThrottle = (slotCount: number, paceMs: number, burstCount: number): Throttle => {
  const slots = Math.max(1, slotCount);
  const pace = Math.max(1, paceMs);
  const burst = Math.max(1, burstCount);
  let queue: Deferred[] = [];
  let active = 0;
  let tokens = burst;
  let updatedAt = Date.now();
  let until = 0;

  const refill = () => {
    const now = Date.now();
    const elapsed = now - updatedAt;
    const nextTokens = Math.floor(elapsed / pace);

    if (nextTokens > 0) {
      tokens = Math.min(burst, tokens + nextTokens);
      updatedAt += nextTokens * pace;
    }
  };

  const waitForCooldown = async (signal?: AbortSignal) => {
    const delay = until - Date.now();
    if (delay > 0) {
      await sleep(delay, signal);
    }
  };

  const waitForToken = async (signal?: AbortSignal): Promise<void> => {
    refill();

    if (tokens >= 1) {
      tokens -= 1;
      return;
    }

    await sleep(pace, signal);
    return waitForToken(signal);
  };

  const acquire = async (signal?: AbortSignal) => {
    if (signal?.aborted) {
      throw signal.reason ?? new DOMException('operation aborted', 'AbortError');
    }

    await waitForCooldown(signal);
    await waitForToken(signal);

    if (active < slots) {
      active += 1;
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const deferred = { resolve, reject };
      const abort = () => {
        queue = queue.filter((item) => item !== deferred);
        reject(signal?.reason ?? new DOMException('operation aborted', 'AbortError'));
      };

      signal?.addEventListener('abort', abort, { once: true });
      queue.push({
        resolve: () => {
          signal?.removeEventListener('abort', abort);
          active += 1;
          resolve();
        },
        reject
      });
    });
  };

  const release = () => {
    active = Math.max(0, active - 1);
    const next = queue.shift();
    next?.resolve();
  };

  const postpone = (wait: number) => {
    until = Math.max(until, Date.now() + wait);
  };

  return {
    acquire,
    release,
    postpone
  };
};

let generate = useThrottle(3, 1000, 3);
let embedding = useThrottle(6, Math.floor(1000 / 3), 6);

export const newGenkit = async (config: Config) => {
  engine = genkit({
    plugins: [
      vertexAI({
        projectId: config.project,
        location: config.location
      })
    ]
  });

  model = config.generate.model;
  embedder = config.embed.model;
  temperature = config.temperature;

  generate = useThrottle(config.generate.slots, config.generate.pace, config.generate.burst);
  embedding = useThrottle(config.embed.slots, config.embed.pace, config.embed.burst);
};

export const run = async <Result, Input = unknown>(
  ctx: RuntimeContext,
  prompt: PromptTemplate<Input>,
  input: Input,
  modelOverride?: Model
): Promise<Result> => {
  ensureInitialized();

  const promptText = renderPrompt(prompt, input);
  const modelToUse = modelOverride || model;
  const signal = signalFrom(ctx);
  const ai = ensureInitialized();

  return retry(signal, generate, async () => {
    const response = await ai.generate({
      model: vertexAI.model(stripVertexPrefix(modelToUse)),
      prompt: promptText,
      config: {
        temperature
      }
    });

    return extractGeneratedData<Result>(response);
  });
};

export const embed = async (ctx: RuntimeContext, text: string): Promise<number[]> => {
  const ai = ensureInitialized();

  const signal = signalFrom(ctx);

  return retry(signal, embedding, async () => {
    const response = await ai.embed({
      embedder: vertexAI.embedder(stripVertexPrefix(embedder) as `${string}embedding${string}`),
      content: text
    });

    const vector = extractEmbedding(response);
    if (!vector.length) {
      throw new Error('google: embedding response contained no embeddings');
    }

    return vector;
  });
};

const retry = async <T>(signal: AbortSignal | undefined, throttle: Throttle | undefined, call: () => Promise<T>): Promise<T> => {
  for (let step = 0; step < 10; step += 1) {
    let acquired = false;

    if (throttle) {
      await throttle.acquire(signal);
      acquired = true;
    }

    try {
      return await call();
    } catch (error) {
      if (acquired) {
        throttle?.release();
        acquired = false;
      }

      if (!limited(error)) {
        throw error;
      }

      if (step === 9) {
        throw new Error('google: resource exhausted', { cause: error });
      }

      const base = Math.min(2 ** step * 1000, 30_000);
      const wait = base + Math.floor(Math.random() * (base / 2));
      throttle?.postpone(wait);
      await sleep(wait, signal);
    } finally {
      if (acquired) {
        throttle?.release();
      }
    }
  }

  throw new Error('google: retry loop exhausted');
};

const limited = (error: unknown) => {
  if (!error) {
    return false;
  }

  const message = error instanceof Error ? error.message : String(error);
  return message.includes('429') || message.includes('RESOURCE_EXHAUSTED');
};

const renderPrompt = <Input>(prompt: PromptTemplate<Input>, input: Input) => {
  if (typeof prompt === 'string') {
    return prompt;
  }

  if (typeof prompt === 'function') {
    return prompt(input);
  }

  return prompt.render(input);
};

const extractGeneratedData = <Result>(response: any): Result => {
  if (response?.output !== undefined) {
    return response.output as Result;
  }

  if (response?.data !== undefined) {
    return response.data as Result;
  }

  if (typeof response?.text === 'string') {
    return JSON.parse(response.text) as Result;
  }

  if (typeof response?.text === 'function') {
    return JSON.parse(response.text()) as Result;
  }

  return response as Result;
};

const extractEmbedding = (response: any): number[] => {
  const vector =
    response?.embedding ??
    response?.embeddings?.[0]?.embedding ??
    response?.embeddings?.[0]?.values ??
    response?.[0]?.embedding ??
    response?.[0]?.values;

  return Array.isArray(vector) ? vector : [];
};

const ensureInitialized = () => {
  if (!engine) {
    throw new Error('google: genkit runtime has not been initialized');
  }

  return engine;
};

const stripVertexPrefix = (name: string) => {
  return name.replace(/^vertexai\//, '');
};

const signalFrom = (ctx: RuntimeContext) => {
  return ctx instanceof AbortSignal ? ctx : ctx?.signal;
};

const sleep = (ms: number, signal?: AbortSignal) => {
  if (signal?.aborted) {
    return Promise.reject(signal.reason ?? new DOMException('operation aborted', 'AbortError'));
  }

  return new Promise<void>((resolve, reject) => {
    const timeout = globalThis.setTimeout(resolve, ms);
    const abort = () => {
      globalThis.clearTimeout(timeout);
      reject(signal?.reason ?? new DOMException('operation aborted', 'AbortError'));
    };

    signal?.addEventListener('abort', abort, { once: true });
  });
};

export const NewGenkit = newGenkit;
export const Run = run;
export const Embed = embed;
