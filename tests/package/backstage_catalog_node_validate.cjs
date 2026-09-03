"use strict";

const MAX_INPUT_BYTES = 4 * 1024 * 1024;
const MAX_DOCUMENTS = 1000;
const EXPECTED_CATALOG_MODEL_VERSION = "1.10.0";
const EXPECTED_YAML_VERSION = "2.8.1";

let failureReported = false;

function fail(message) {
  if (failureReported) {
    return;
  }
  failureReported = true;
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
}

let YAML;
let catalogModel;
try {
  YAML = require("yaml");
  catalogModel = require("@backstage/catalog-model");
} catch (_error) {
  fail("Backstage parity validator dependencies are unavailable");
  return;
}

if (
  require("@backstage/catalog-model/package.json").version !==
    EXPECTED_CATALOG_MODEL_VERSION ||
  require("yaml/package.json").version !== EXPECTED_YAML_VERSION
) {
  fail("Backstage parity validator dependency versions do not match their pins");
  return;
}

const validators = new Map([
  ["System", catalogModel.systemEntityV1alpha1Validator],
  ["Resource", catalogModel.resourceEntityV1alpha1Validator],
  ["Component", catalogModel.componentEntityV1alpha1Validator],
]);

if (
  [...validators.values()].some(
    validator => validator === undefined || typeof validator.check !== "function",
  )
) {
  fail("Backstage parity validator exports are unavailable");
  return;
}

let input = "";
let inputBytes = 0;
let inputTooLarge = false;
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => {
  inputBytes += Buffer.byteLength(chunk, "utf8");
  if (inputBytes > MAX_INPUT_BYTES) {
    inputTooLarge = true;
    input = "";
    return;
  }
  if (!inputTooLarge) {
    input += chunk;
  }
});
process.stdin.on("error", () => fail("Could not read Backstage YAML input"));
process.stdin.on("end", async () => {
  if (failureReported) {
    return;
  }
  if (inputTooLarge) {
    fail("Backstage YAML input exceeds the validation limit");
    return;
  }

  let documents;
  try {
    documents = YAML.parseAllDocuments(input, {
      merge: false,
      prettyErrors: false,
      strict: true,
      uniqueKeys: true,
    });
  } catch (_error) {
    fail("Backstage YAML input could not be parsed");
    return;
  }

  if (documents.length === 0) {
    fail("Backstage YAML input contains no documents");
    return;
  }
  if (documents.length > MAX_DOCUMENTS) {
    fail("Backstage YAML input contains too many documents");
    return;
  }

  for (let index = 0; index < documents.length; index += 1) {
    const document = documents[index];
    if (document.errors.length > 0) {
      fail(`Backstage YAML document ${index + 1} is invalid`);
      return;
    }

    let entity;
    try {
      entity = document.toJS({ maxAliasCount: 0 });
    } catch (_error) {
      fail(`Backstage YAML document ${index + 1} is invalid`);
      return;
    }
    if (entity === null || typeof entity !== "object" || Array.isArray(entity)) {
      fail(`Backstage YAML document ${index + 1} is not an entity`);
      return;
    }

    const validator = validators.get(entity.kind);
    if (validator === undefined) {
      fail(`Backstage YAML document ${index + 1} has an unsupported kind`);
      return;
    }

    let accepted;
    try {
      accepted = await validator.check(entity);
    } catch (_error) {
      fail(`Backstage YAML document ${index + 1} failed schema validation`);
      return;
    }
    if (accepted !== true) {
      fail(`Backstage YAML document ${index + 1} failed schema validation`);
      return;
    }
  }
});
