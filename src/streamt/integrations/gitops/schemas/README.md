# Vendored Strimzi schema

`strimzi-1.2.0-kafkatopic-crd.yaml.gz.b64` is a deterministic
gzip-compressed, base64-encoded copy of the exact KafkaTopic CRD distributed by
Strimzi Kafka Operator 1.2.0. It is sufficient for offline structural
validation without a Kubernetes, Strimzi, Helm, or OpenShift Python dependency.

- Upstream release: `1.2.0` (released 2026-08-20)
- Upstream commit: `6c7b43c4af0db547c10463ba09d1dfa6f5e156a0`
- Upstream path: `packaging/install/cluster-operator/043-Crd-kafkatopic.yaml`
- Source URL: <https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/6c7b43c4af0db547c10463ba09d1dfa6f5e156a0/packaging/install/cluster-operator/043-Crd-kafkatopic.yaml>
- Uncompressed size: `6329` bytes
- SHA-256 after decoding and decompressing:
  `36390f0731c699448076d4ee739e8b7f331d083e91a7fb71500aaa830ab1127e`
- Retrieval date: `2026-09-03`
- Transformation: exact upstream bytes, gzip-compressed with the gzip
  modification time fixed at zero, then base64-encoded with line wrapping
- License: Apache License 2.0

`strimzi-1.2.0-LICENSE.txt` is an exact copy of the upstream root
`LICENSE` at the pinned commit.
`strimzi-1.2.0-NOTICE.txt` records the complete provenance and transformation;
the upstream repository has no root `NOTICE` file at the pinned commit.

Do not edit the encoded payload by hand. Any target change must pin a reviewed
release and immutable commit, retrieve and verify the exact CRD bytes, update
the decoded size and SHA-256, reproduce gzip with a zero modification time,
and retain the license, notice, distribution-integrity, and dependency/import
boundary tests.
