import { useAppConfig } from '$config';
import { newGenkit } from './genkit';

let initialized = false;

export const useGoogleGenkit = async () => {
  if (initialized) {
    return;
  }

  await newGenkit(useAppConfig().genkit);
  initialized = true;
};
