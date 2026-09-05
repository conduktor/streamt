package dev.streamt.proof;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/** A closed, versioned plan contract. Unknown fields and operations are errors. */
public final class Plan {
    static final ObjectMapper JSON = JsonMapper.builder()
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private final JsonNode root;

    public Plan(byte[] bytes) {
        root = parse(bytes);
        fields(root, Set.of("version", "input_topic", "output_topic", "schema", "projection", "predicates"));
        require(root.get("version").isIntegralNumber() && root.get("version").intValue() == 1
            && root.get("version").canConvertToInt(), "Unsupported plan version");
        for (String field : Set.of("input_topic", "output_topic")) {
            require(root.get(field).isTextual() && root.get(field).textValue()
                .matches("[A-Za-z0-9][A-Za-z0-9_.-]{0,248}"), "Invalid topic");
        }
        require(!inputTopic().equals(outputTopic()), "Input and output topics must differ");
        JsonNode schema = root.get("schema");
        require(schema.isObject() && !schema.isEmpty(), "Nonempty schema required");
        schema.fields().forEachRemaining(entry -> {
            name(entry.getKey());
            fields(entry.getValue(), Set.of("type", "nullable"));
            require(entry.getValue().get("type").isTextual()
                && Set.of("STRING", "BIGINT", "BOOLEAN").contains(entry.getValue().get("type").textValue()), "Unsupported type");
            require(entry.getValue().get("nullable").isBoolean(), "nullable must be boolean");
        });
        require(root.get("projection").isArray() && !root.get("projection").isEmpty(), "Nonempty projection required");
        Set<String> aliases = new HashSet<>();
        for (JsonNode projection : root.get("projection")) {
            fields(projection, Set.of("column", "as"));
            column(projection.get("column"));
            require(projection.get("as").isTextual(), "Invalid alias");
            name(projection.get("as").textValue());
            require(aliases.add(projection.get("as").textValue()), "Duplicate output alias");
        }
        require(root.get("predicates").isArray(), "Predicates must be an array");
        for (JsonNode predicate : root.get("predicates")) {
            require(predicate.isObject() && predicate.has("op") && predicate.get("op").isTextual(), "Invalid predicate");
            String op = predicate.get("op").textValue();
            require(Set.of("eq", "ne", "gt", "ge", "lt", "le", "is_null", "not_null").contains(op), "Unsupported operator");
            boolean nullCheck = op.equals("is_null") || op.equals("not_null");
            fields(predicate, nullCheck ? Set.of("column", "op") : Set.of("column", "op", "value"));
            JsonNode field = column(predicate.get("column"));
            if (!nullCheck) {
                require(!predicate.get("value").isNull(), "Use IS NULL instead of comparison with NULL");
                typed(predicate.get("value"), field);
                require(Set.of("eq", "ne").contains(op) || field.get("type").textValue().equals("BIGINT"), "Only BIGINT supports ordering");
            }
        }
    }

    public String inputTopic() { return root.get("input_topic").textValue(); }
    public String outputTopic() { return root.get("output_topic").textValue(); }

    /** Null result means the explicit tombstone/filter drop policy. */
    public byte[] transform(byte[] bytes) {
        if (bytes == null) return null;
        JsonNode row = parse(bytes);
        Set<String> expected = new HashSet<>();
        root.get("schema").fieldNames().forEachRemaining(expected::add);
        fields(row, expected);
        root.get("schema").fields().forEachRemaining(entry -> typed(row.get(entry.getKey()), entry.getValue()));
        for (JsonNode predicate : root.get("predicates")) {
            JsonNode actual = row.get(predicate.get("column").textValue());
            String op = predicate.get("op").textValue();
            boolean matches;
            if (op.equals("is_null")) matches = actual.isNull();
            else if (op.equals("not_null")) matches = !actual.isNull();
            // SQL WHERE keeps only TRUE; an ordinary comparison with NULL is UNKNOWN.
            else if (actual.isNull()) matches = false;
            else {
                JsonNode expectedValue = predicate.get("value");
                int compare = actual.isIntegralNumber() ? Long.compare(actual.longValue(), expectedValue.longValue()) : 0;
                boolean equal = actual.isIntegralNumber() ? compare == 0 : actual.equals(expectedValue);
                matches = switch (op) {
                    case "eq" -> equal;
                    case "ne" -> !equal;
                    case "gt" -> compare > 0;
                    case "ge" -> compare >= 0;
                    case "lt" -> compare < 0;
                    case "le" -> compare <= 0;
                    default -> throw new IllegalStateException("Unvalidated operation");
                };
            }
            if (!matches) return null;
        }
        ObjectNode output = JSON.createObjectNode();
        for (JsonNode projection : root.get("projection")) {
            output.set(projection.get("as").textValue(), row.get(projection.get("column").textValue()));
        }
        try { return JSON.writeValueAsBytes(output); }
        catch (IOException error) { throw new IllegalArgumentException("Cannot serialize output", error); }
    }

    private JsonNode column(JsonNode column) {
        require(column.isTextual() && root.get("schema").has(column.textValue()), "Unknown column");
        return root.get("schema").get(column.textValue());
    }

    private static void typed(JsonNode value, JsonNode field) {
        if (value.isNull()) {
            require(field.get("nullable").booleanValue(), "NULL in non-nullable field");
            return;
        }
        boolean valid = switch (field.get("type").textValue()) {
            case "STRING" -> value.isTextual();
            case "BOOLEAN" -> value.isBoolean();
            case "BIGINT" -> value.isIntegralNumber() && value.canConvertToLong();
            default -> false;
        };
        require(valid, "Record/literal type mismatch");
    }

    private static JsonNode parse(byte[] bytes) {
        try {
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
            JsonNode node = JSON.readTree(text);
            require(node != null, "Empty JSON");
            return node;
        } catch (CharacterCodingException error) {
            throw new IllegalArgumentException("Value must be UTF-8", error);
        } catch (IOException error) {
            // Do not echo event data into the application log.
            throw new IllegalArgumentException("Invalid JSON value");
        }
    }

    private static void name(String name) {
        require(name.matches("[a-z_][a-z0-9_]*"), "Invalid column name");
    }

    private static void fields(JsonNode node, Set<String> expected) {
        require(node.isObject(), "Expected JSON object");
        Set<String> actual = new HashSet<>();
        node.fieldNames().forEachRemaining(actual::add);
        require(actual.equals(expected), "Missing or unknown JSON fields");
    }

    private static void require(boolean valid, String message) {
        if (!valid) throw new IllegalArgumentException(message);
    }
}
