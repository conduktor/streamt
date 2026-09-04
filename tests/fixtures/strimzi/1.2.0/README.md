# Strimzi 1.2.0 reviewed fixture contract

These fixtures freeze the proposed offline `KafkaTopic` export contract before
the mapper or CLI exists. They are test evidence, not a support claim.

- `manifest.json` exercises direct and full-hash Kubernetes names, an omitted
  external topic, every counted non-topic artifact kind, and string, boolean,
  and integer topic configuration values.
- The secret-only and non-secret variants prove the checksum's intended
  stability and sensitivity boundaries.
- `expected-documents.json` and `expected.yaml` are the same reviewed document
  tuple in structural and canonical-byte form.
- The empty fixtures freeze an empty document tuple and a genuinely zero-byte
  YAML stream.
- `contract.json` freezes checksums, the full metadata-name digest, exact
  warning objects and order, exact counts, and output byte digests.

Confidential sentinel values intentionally occur only in manifest inputs and
the fixture contract's sentinel inventory. They must never occur in expected
documents, YAML, warnings, or future command surfaces.
