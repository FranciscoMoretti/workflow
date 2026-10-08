# ChatJS PostgreSQL World

This fork maintains the PostgreSQL provider used by ChatJS. The proposed registry release is `@chat-js/workflow-world-postgres@5.0.0-beta.40-chatjs.1`, installed through the `@workflow/world-postgres` npm alias. It is not yet published.

Build the World dependency graph, then run `node scripts/pack-chatjs-world.mjs`. The script copies source-built runtime output and migrations, resolves workspace dependencies to explicit public package versions, and removes development lifecycle scripts. The ChatJS PostgreSQL World workflow validates upstream tests and the registry against PostgreSQL, then uploads the tarball and SHA256SUMS. It does not publish.

Release requires review of the exact source, successful CI, verification of the packed manifest and runtime exports, and fresh ChatJS native/PG/installer acceptance against those exact bytes. Publish only the verified archive, then verify registry integrity and a clean consumer install/lockfile. Earlier experimental registry archives are retained evidence and must not be relabeled.

Registry schema drafts are not an in-place production migration. Stop old producers and drain or quarantine legacy data before claiming complete inventory. Managed Vercel and arbitrary auxiliary resource cleanup remain unsupported.
