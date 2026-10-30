import federation from '@originjs/vite-plugin-federation'
import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vite'
import { readFile, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
export default defineConfig({
  plugins: [vue(), federation({
    name: 'EmptyFolderCleaner', filename: 'remoteEntry.js',
    exposes: { './Page': './src/components/Page.vue', './Config': './src/components/Config.vue' },
    // Vuetify components are resolved from the host application registry.
    // Sharing its CSS here would inject a second complete host stylesheet.
    shared: Object.fromEntries(['vue'].map(name => [name, {
      requiredVersion: false, generate: false, singleton: true,
    }])),
    format: 'esm',
  }), {
    name: 'cleanup-federation-entry-whitespace',
    async writeBundle(options) {
      const entry = resolve(options.dir || 'dist', 'assets/remoteEntry.js')
      const source = await readFile(entry, 'utf8')
      const normalized = source.replace(/[\t ]+(?=\r?$)/gm, '')
      if (normalized !== source) await writeFile(entry, normalized)
    },
  }],
  build: { target: 'esnext', minify: false, cssCodeSplit: true },
})
