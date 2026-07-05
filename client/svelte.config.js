import adapter from '@sveltejs/adapter-auto';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

const config = {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter(),
    alias: {
      '$adapters': 'src/adapters',
      '$agents': 'src/features/agents',
      '$apis': 'src/apis',
      '$components': 'src/components',
      '$config': 'src/config',
      '$domains': 'src/domains',
      '$features': 'src/features',
      '$hooks': 'src/hooks',
      '$ui': 'src/components/ui',
      '$utils': 'src/utils'
    }
  }
};

export default config;
