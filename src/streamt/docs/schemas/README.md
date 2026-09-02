# Vendored standards schemas

## AsyncAPI 3.1

`asyncapi-3.1.0-without-id.json.gz.b64` is a gzip-compressed, base64-encoded
copy of the official `3.1.0-without-$id.json` schema from
[`asyncapi/spec-json-schemas`](https://github.com/asyncapi/spec-json-schemas).
It is encoded as text so the package can carry the exact upstream schema as a
portable import resource.

- Upstream commit: `61cc6add7cf3467f56d1fbb55b1a2b78b4ae6323`
- Upstream path: `schemas/3.1.0-without-$id.json`
- SHA-256 after decoding and decompressing:
  `44c97489a276ad11f6f9edcbdbde47af8b66f35f97f2af9f3a289e9e24c584c5`
- License: Apache License 2.0, as declared by the upstream repository

Do not edit the encoded payload by hand. Any update must pin an official
released AsyncAPI schema, update the commit and checksum above, and retain the
offline validation tests.

## Open Data Contract Standard 3.1

`odcs-3.1.0.json.gz.b64` is a gzip-compressed, base64-encoded copy of the
official `odcs-json-schema-v3.1.0.json` companion schema from the ODCS v3.1.0
release. The textual ODCS standard remains authoritative when it differs from
this companion schema, so streamt also applies the semantic checks specified in
`docs/specs/odcs-export.md`.

- Upstream release: `v3.1.0`
- Upstream commit: `b9d3ffc5aabe9e058afe4469cabe5a218fe9946d`
- Upstream path: `schema/odcs-json-schema-v3.1.0.json`
- Uncompressed size: `86441` bytes
- SHA-256 after decoding and decompressing:
  `2cb7dd6fe43344d2233e0406438622681dc3ebadcf8f0d606a15b40c8f6752c0`
- License: Apache License 2.0, as declared by the upstream repository

Do not edit the encoded payload by hand. Any update must target an approved
ODCS release, update the pinned commit, size, and checksum above, and retain the
official-schema and local-semantic offline validation tests.
