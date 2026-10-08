import { execFileSync } from 'node:child_process';
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const source = join(root, 'packages/world-postgres');
const output = resolve(process.argv[2] ?? join(root, 'artifacts'));
const stage = await mkdtemp(join(tmpdir(), 'chatjs-world-pack-'));
const manifest = JSON.parse(
  await readFile(join(source, 'package.json'), 'utf8')
);
const resolvedDependencies = {
  '@vercel/queue': '0.5.0',
  '@workflow/errors': '5.0.0-beta.20',
  '@workflow/utils': '5.0.0-beta.10',
  '@workflow/world': '5.0.0-beta.33',
  '@workflow/world-local': '5.0.0-beta.42',
  ulid: '~3.0.1',
  zod: '~4.3.6',
};

try {
  for (const file of manifest.files) {
    await cp(join(source, file), join(stage, file), { recursive: true });
  }
  await cp(join(root, 'LICENSE'), join(stage, 'LICENSE'));
  await cp(join(source, 'README.md'), join(stage, 'README.md'));
  manifest.name = '@chat-js/workflow-world-postgres';
  manifest.version = '5.0.0-beta.40-chatjs.1';
  manifest.repository.url = 'https://github.com/FranciscoMoretti/workflow.git';
  manifest.publishConfig = { access: 'public', tag: 'chatjs' };
  manifest.dependencies = { ...manifest.dependencies, ...resolvedDependencies };
  if (
    Object.values(manifest.dependencies).some((value) =>
      /^(workspace|catalog):/.test(value)
    )
  ) {
    throw new Error('Unresolved workspace dependency in distributable');
  }
  delete manifest.devDependencies;
  delete manifest.scripts;
  delete manifest.packageManager;
  await writeFile(
    join(stage, 'package.json'),
    `${JSON.stringify(manifest, null, 2)}\n`
  );
  await mkdir(output, { recursive: true });
  execFileSync('bun', ['pm', 'pack', '--destination', output], {
    cwd: stage,
    stdio: 'inherit',
  });
} finally {
  await rm(stage, { recursive: true, force: true });
}
