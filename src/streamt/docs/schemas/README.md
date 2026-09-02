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

## OpenLineage 1.53.0

The nine `openlineage-1.53.0-*.json.gz.b64` resources are deterministic
gzip-compressed, base64-encoded copies of the official OpenLineage core schema
and the eight standard facet schemas emitted by streamt. They come from the
[`OpenLineage/OpenLineage`](https://github.com/OpenLineage/OpenLineage)
repository and are sufficient for validation without network access or an
OpenLineage runtime dependency.

- Upstream release: `1.53.0`
- Annotated tag object: `48302e12018aea164bd903283b6841a939d8ef1f`
- Upstream commit: `8ad5c14c63fbab63fedd8ff42f9a208d86ad07fe`
- Total uncompressed size: `21110` bytes
- License: Apache License 2.0, as declared by the upstream repository

| Resource suffix | Upstream path | Official `$id` | Bytes | SHA-256 after decoding and decompressing |
| --- | --- | --- | ---: | --- |
| `core` | `spec/OpenLineage.json` | `https://openlineage.io/spec/2-0-2/OpenLineage.json` | 9155 | `69f68bee00b9beac88a87059c0102410e7bb05f3f43c46d02a0409831eceb0d2` |
| `job-type-job-facet` | `spec/facets/JobTypeJobFacet.json` | `https://openlineage.io/spec/facets/2-0-4/JobTypeJobFacet.json` | 3072 | `11c12cab95a411ca31066c80d2bb4aefd37bbcadbda5e4d343d2069853b907d5` |
| `schema-dataset-facet` | `spec/facets/SchemaDatasetFacet.json` | `https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json` | 1687 | `50236a779aa64baa0bad0055391838bd22fcb36ce667c41d60ada80915e899b6` |
| `documentation-dataset-facet` | `spec/facets/DocumentationDatasetFacet.json` | `https://openlineage.io/spec/facets/1-1-0/DocumentationDatasetFacet.json` | 1031 | `bad5c041d679e73b2faea43506860428f48d80710ad1354a2d2393475143285f` |
| `documentation-job-facet` | `spec/facets/DocumentationJobFacet.json` | `https://openlineage.io/spec/facets/1-1-0/DocumentationJobFacet.json` | 944 | `b5823685c20c712d9ee1e3b310ad6ca426be7b46db60acd9f23248811af3c8d4` |
| `dataset-type-dataset-facet` | `spec/facets/DatasetTypeDatasetFacet.json` | `https://openlineage.io/spec/facets/1-0-1/DatasetTypeDatasetFacet.json` | 1008 | `1a7d5106877151d52c4d3967f77b92788ea42ebf9843c6573d776a63c9a7d157` |
| `ownership-dataset-facet` | `spec/facets/OwnershipDatasetFacet.json` | `https://openlineage.io/spec/facets/1-0-1/OwnershipDatasetFacet.json` | 1368 | `9a18a508746627eff18fa84ca1694e1a9f2d556ac0052dbdb6970d8a35f75231` |
| `ownership-job-facet` | `spec/facets/OwnershipJobFacet.json` | `https://openlineage.io/spec/facets/1-0-1/OwnershipJobFacet.json` | 1344 | `c54ae771b1183efbe007a778eb5bf05e9e3f39f3d48f22a4beceea00950abdad` |
| `error-message-run-facet` | `spec/facets/ErrorMessageRunFacet.json` | `https://openlineage.io/spec/facets/1-0-1/ErrorMessageRunFacet.json` | 1501 | `b11b4ee8b0f99f6846264f87cad48390255c7ef142ce623216660c7097be0ea6` |

Do not edit the encoded payloads by hand. An update must pin an official
release and commit, reproduce gzip with `mtime=0`, update every decoded size and
checksum above, and keep integrity, package-resource, and no-network tests. The
validator deliberately has no remote schema fallback.
