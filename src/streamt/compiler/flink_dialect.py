"""Flink SQL dialect for sqlglot.

This module defines a custom Flink SQL dialect for sqlglot to properly parse
and handle Flink-specific SQL patterns including:
- TUMBLE, HOP, SESSION, CUMULATE windowing TVFs
- TUMBLE_START, TUMBLE_END, etc. time attribute functions
- PROCTIME() for processing time
- Flink-specific data types
- MATCH_RECOGNIZE for Complex Event Processing (CEP)
- FOR SYSTEM_TIME AS OF for temporal/versioned table joins
- TABLE() TVF with CUMULATE, TUMBLE, HOP windowing functions

Confluent Cloud for Apache Flink compatibility:
- JSON functions: JSON_VALUE, JSON_QUERY, JSON_EXISTS, JSON_ARRAY, JSON_OBJECT, etc.
- Datetime functions: TO_TIMESTAMP_LTZ, TIMESTAMPADD, TIMESTAMPDIFF, CONVERT_TZ, etc.
- Aggregate functions: LISTAGG, COLLECT, STDDEV_POP, VAR_SAMP, CUME_DIST, etc.
- String functions: REGEXP_EXTRACT, SPLIT_INDEX, STR_TO_MAP, PARSE_URL, etc.
- Array functions: UNNEST, CARDINALITY, ELEMENT
- ML functions: ML_PREDICT, ML_EVALUATE for model inference
"""

from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.generator import Generator
from sqlglot.parser import Parser
from sqlglot.tokens import Tokenizer, TokenType


class FlinkDialect(Dialect):
    """Custom Flink SQL dialect for sqlglot.

    This dialect extends sqlglot's default dialect to support Flink SQL syntax,
    including windowing functions, MATCH_RECOGNIZE for CEP, temporal joins,
    and Flink-specific types.
    """

    class Tokenizer(Tokenizer):
        """Flink SQL tokenizer with custom keywords."""

        QUOTES = ["'"]
        IDENTIFIERS = ["`", '"']

        KEYWORDS = {
            **Tokenizer.KEYWORDS,
            # Flink-specific types
            "STRING": TokenType.VARCHAR,
            "BYTES": TokenType.VARBINARY,
            "TINYINT": TokenType.TINYINT,
            "SMALLINT": TokenType.SMALLINT,
            "INT": TokenType.INT,
            "INTEGER": TokenType.INT,
            "BIGINT": TokenType.BIGINT,
            "FLOAT": TokenType.FLOAT,
            "DOUBLE": TokenType.DOUBLE,
            "DECIMAL": TokenType.DECIMAL,
            "BOOLEAN": TokenType.BOOLEAN,
            "DATE": TokenType.DATE,
            "TIME": TokenType.TIME,
            "TIMESTAMP": TokenType.TIMESTAMP,
            "TIMESTAMP_LTZ": TokenType.TIMESTAMPLTZ,
            "TIMESTAMP WITH LOCAL TIME ZONE": TokenType.TIMESTAMPLTZ,
            "TIMESTAMP WITH TIME ZONE": TokenType.TIMESTAMPTZ,
            "ARRAY": TokenType.ARRAY,
            "MAP": TokenType.MAP,
            "ROW": TokenType.STRUCT,
            "MULTISET": TokenType.ARRAY,
            # MATCH_RECOGNIZE for Complex Event Processing
            "MATCH_RECOGNIZE": TokenType.MATCH_RECOGNIZE,
            # Temporal join support (FOR SYSTEM_TIME AS OF)
            "FOR SYSTEM_TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "FOR SYSTEM TIME": TokenType.TIMESTAMP_SNAPSHOT,
            "FOR SYSTEM_TIME AS OF": TokenType.TIMESTAMP_SNAPSHOT,
            "FOR SYSTEM TIME AS OF": TokenType.TIMESTAMP_SNAPSHOT,
        }

    class Parser(Parser):
        """Flink SQL parser with custom function and syntax support."""

        FUNCTIONS = {
            **Parser.FUNCTIONS,
            # Map Flink window functions to Anonymous (sqlglot doesn't have these)
            # They will be parsed as exp.Anonymous which is fine for type inference
            "TUMBLE": lambda args: exp.Anonymous(this="TUMBLE", expressions=args),
            "TUMBLE_START": lambda args: exp.Anonymous(this="TUMBLE_START", expressions=args),
            "TUMBLE_END": lambda args: exp.Anonymous(this="TUMBLE_END", expressions=args),
            "TUMBLE_ROWTIME": lambda args: exp.Anonymous(this="TUMBLE_ROWTIME", expressions=args),
            "TUMBLE_PROCTIME": lambda args: exp.Anonymous(this="TUMBLE_PROCTIME", expressions=args),
            "HOP": lambda args: exp.Anonymous(this="HOP", expressions=args),
            "HOP_START": lambda args: exp.Anonymous(this="HOP_START", expressions=args),
            "HOP_END": lambda args: exp.Anonymous(this="HOP_END", expressions=args),
            "HOP_ROWTIME": lambda args: exp.Anonymous(this="HOP_ROWTIME", expressions=args),
            "HOP_PROCTIME": lambda args: exp.Anonymous(this="HOP_PROCTIME", expressions=args),
            "SESSION": lambda args: exp.Anonymous(this="SESSION", expressions=args),
            "SESSION_START": lambda args: exp.Anonymous(this="SESSION_START", expressions=args),
            "SESSION_END": lambda args: exp.Anonymous(this="SESSION_END", expressions=args),
            "SESSION_ROWTIME": lambda args: exp.Anonymous(this="SESSION_ROWTIME", expressions=args),
            "SESSION_PROCTIME": lambda args: exp.Anonymous(this="SESSION_PROCTIME", expressions=args),
            # Processing time
            "PROCTIME": lambda args: exp.Anonymous(this="PROCTIME", expressions=args),
            # Flink built-in functions
            "TO_TIMESTAMP": exp.TsOrDsToTimestamp.from_arg_list,
            "FROM_UNIXTIME": exp.UnixToTime.from_arg_list,
            "UNIX_TIMESTAMP": exp.TimeToUnix.from_arg_list,
            "DATE_FORMAT": exp.TimeToStr.from_arg_list,
            "CONCAT": exp.Concat.from_arg_list,
            "CONCAT_WS": exp.ConcatWs.from_arg_list,
            "COALESCE": exp.Coalesce.from_arg_list,
            "NULLIF": exp.Nullif.from_arg_list,
            "IF": exp.If.from_arg_list,
            # JSON functions (Confluent Flink compatible)
            "JSON_VALUE": lambda args: exp.Anonymous(this="JSON_VALUE", expressions=args),
            "JSON_QUERY": lambda args: exp.Anonymous(this="JSON_QUERY", expressions=args),
            "JSON_EXISTS": lambda args: exp.Anonymous(this="JSON_EXISTS", expressions=args),
            "JSON_ARRAY": lambda args: exp.Anonymous(this="JSON_ARRAY", expressions=args),
            "JSON_OBJECT": lambda args: exp.Anonymous(this="JSON_OBJECT", expressions=args),
            "JSON_STRING": lambda args: exp.Anonymous(this="JSON_STRING", expressions=args),
            "JSON_ARRAYAGG": lambda args: exp.Anonymous(this="JSON_ARRAYAGG", expressions=args),
            "JSON_OBJECTAGG": lambda args: exp.Anonymous(this="JSON_OBJECTAGG", expressions=args),
            "JSON_QUOTE": lambda args: exp.Anonymous(this="JSON_QUOTE", expressions=args),
            "JSON_UNQUOTE": lambda args: exp.Anonymous(this="JSON_UNQUOTE", expressions=args),
            "IS_JSON": lambda args: exp.Anonymous(this="IS_JSON", expressions=args),
            # Datetime functions (Confluent Flink compatible)
            "TO_TIMESTAMP_LTZ": lambda args: exp.Anonymous(this="TO_TIMESTAMP_LTZ", expressions=args),
            "CURRENT_WATERMARK": lambda args: exp.Anonymous(this="CURRENT_WATERMARK", expressions=args),
            "SOURCE_WATERMARK": lambda args: exp.Anonymous(this="SOURCE_WATERMARK", expressions=args),
            "TIMESTAMPADD": lambda args: exp.Anonymous(this="TIMESTAMPADD", expressions=args),
            "TIMESTAMPDIFF": lambda args: exp.Anonymous(this="TIMESTAMPDIFF", expressions=args),
            "CONVERT_TZ": lambda args: exp.Anonymous(this="CONVERT_TZ", expressions=args),
            "TO_DATE": lambda args: exp.Anonymous(this="TO_DATE", expressions=args),
            "DAYOFWEEK": lambda args: exp.Anonymous(this="DAYOFWEEK", expressions=args),
            "DAYOFYEAR": lambda args: exp.Anonymous(this="DAYOFYEAR", expressions=args),
            "WEEKOFYEAR": lambda args: exp.Anonymous(this="WEEKOFYEAR", expressions=args),
            "QUARTER": lambda args: exp.Anonymous(this="QUARTER", expressions=args),
            # Aggregate functions (Confluent Flink compatible)
            "LISTAGG": lambda args: exp.Anonymous(this="LISTAGG", expressions=args),
            "COLLECT": lambda args: exp.Anonymous(this="COLLECT", expressions=args),
            "STDDEV_POP": exp.StddevPop.from_arg_list,
            "STDDEV_SAMP": exp.StddevSamp.from_arg_list,
            "VAR_POP": exp.VariancePop.from_arg_list,
            "VAR_SAMP": exp.Variance.from_arg_list,
            "VARIANCE": exp.Variance.from_arg_list,
            "CUME_DIST": exp.CumeDist.from_arg_list,
            "PERCENT_RANK": exp.PercentRank.from_arg_list,
            "FIRST_VALUE": exp.FirstValue.from_arg_list,
            "LAST_VALUE": exp.LastValue.from_arg_list,
            "NTH_VALUE": exp.NthValue.from_arg_list,
            # String functions (Confluent Flink compatible)
            "REGEXP_EXTRACT": lambda args: exp.Anonymous(this="REGEXP_EXTRACT", expressions=args),
            "REGEXP_REPLACE": lambda args: exp.Anonymous(this="REGEXP_REPLACE", expressions=args),
            "REGEXP": lambda args: exp.Anonymous(this="REGEXP", expressions=args),
            "SPLIT_INDEX": lambda args: exp.Anonymous(this="SPLIT_INDEX", expressions=args),
            "STR_TO_MAP": lambda args: exp.Anonymous(this="STR_TO_MAP", expressions=args),
            "PARSE_URL": lambda args: exp.Anonymous(this="PARSE_URL", expressions=args),
            "URL_ENCODE": lambda args: exp.Anonymous(this="URL_ENCODE", expressions=args),
            "URL_DECODE": lambda args: exp.Anonymous(this="URL_DECODE", expressions=args),
            "FROM_BASE64": lambda args: exp.Anonymous(this="FROM_BASE64", expressions=args),
            "TO_BASE64": lambda args: exp.Anonymous(this="TO_BASE64", expressions=args),
            "INITCAP": lambda args: exp.Anonymous(this="INITCAP", expressions=args),
            "INSTR": lambda args: exp.Anonymous(this="INSTR", expressions=args),
            "LOCATE": lambda args: exp.Anonymous(this="LOCATE", expressions=args),
            "LPAD": lambda args: exp.Anonymous(this="LPAD", expressions=args),
            "RPAD": lambda args: exp.Anonymous(this="RPAD", expressions=args),
            "LTRIM": lambda args: exp.Anonymous(this="LTRIM", expressions=args),
            "RTRIM": lambda args: exp.Anonymous(this="RTRIM", expressions=args),
            "BTRIM": lambda args: exp.Anonymous(this="BTRIM", expressions=args),
            "OVERLAY": lambda args: exp.Anonymous(this="OVERLAY", expressions=args),
            "TRANSLATE": lambda args: exp.Anonymous(this="TRANSLATE", expressions=args),
            "ELT": lambda args: exp.Anonymous(this="ELT", expressions=args),
            # Array/Collection functions (Confluent Flink compatible)
            "CARDINALITY": lambda args: exp.Anonymous(this="CARDINALITY", expressions=args),
            "ELEMENT": lambda args: exp.Anonymous(this="ELEMENT", expressions=args),
            "ARRAY_CONCAT": lambda args: exp.Anonymous(this="ARRAY_CONCAT", expressions=args),
            "ARRAY_CONTAINS": lambda args: exp.Anonymous(this="ARRAY_CONTAINS", expressions=args),
            "ARRAY_DISTINCT": lambda args: exp.Anonymous(this="ARRAY_DISTINCT", expressions=args),
            "ARRAY_POSITION": lambda args: exp.Anonymous(this="ARRAY_POSITION", expressions=args),
            "ARRAY_REMOVE": lambda args: exp.Anonymous(this="ARRAY_REMOVE", expressions=args),
            "ARRAY_REVERSE": lambda args: exp.Anonymous(this="ARRAY_REVERSE", expressions=args),
            "ARRAY_SLICE": lambda args: exp.Anonymous(this="ARRAY_SLICE", expressions=args),
            "ARRAY_UNION": lambda args: exp.Anonymous(this="ARRAY_UNION", expressions=args),
            # Map functions
            "MAP_KEYS": lambda args: exp.Anonymous(this="MAP_KEYS", expressions=args),
            "MAP_VALUES": lambda args: exp.Anonymous(this="MAP_VALUES", expressions=args),
            "MAP_ENTRIES": lambda args: exp.Anonymous(this="MAP_ENTRIES", expressions=args),
            "MAP_FROM_ARRAYS": lambda args: exp.Anonymous(this="MAP_FROM_ARRAYS", expressions=args),
            "MAP_UNION": lambda args: exp.Anonymous(this="MAP_UNION", expressions=args),
            # Type conversion
            "TRY_CAST": exp.TryCast.from_arg_list,
            # Flink TVF windowing functions
            "CUMULATE": lambda args: exp.Anonymous(this="CUMULATE", expressions=args),
            # DESCRIPTOR for column references in TVFs
            "DESCRIPTOR": lambda args: exp.Anonymous(this="DESCRIPTOR", expressions=args),
            # ML functions (Confluent Flink AI/ML)
            "ML_PREDICT": lambda args: exp.Anonymous(this="ML_PREDICT", expressions=args),
            "ML_EVALUATE": lambda args: exp.Anonymous(this="ML_EVALUATE", expressions=args),
            # Conditional functions
            "IFNULL": lambda args: exp.Anonymous(this="IFNULL", expressions=args),
            "NVL": lambda args: exp.Anonymous(this="NVL", expressions=args),
            "GREATEST": lambda args: exp.Anonymous(this="GREATEST", expressions=args),
            "LEAST": lambda args: exp.Anonymous(this="LEAST", expressions=args),
            # Math functions
            "LOG2": lambda args: exp.Anonymous(this="LOG2", expressions=args),
            "LOG10": lambda args: exp.Anonymous(this="LOG10", expressions=args),
            "SINH": lambda args: exp.Anonymous(this="SINH", expressions=args),
            "COSH": lambda args: exp.Anonymous(this="COSH", expressions=args),
            "TANH": lambda args: exp.Anonymous(this="TANH", expressions=args),
            "COT": lambda args: exp.Anonymous(this="COT", expressions=args),
            "DEGREES": lambda args: exp.Anonymous(this="DEGREES", expressions=args),
            "RADIANS": lambda args: exp.Anonymous(this="RADIANS", expressions=args),
            "TRUNCATE": lambda args: exp.Anonymous(this="TRUNCATE", expressions=args),
            "RAND_INTEGER": lambda args: exp.Anonymous(this="RAND_INTEGER", expressions=args),
        }

        def _parse_lambda(self, alias: bool = False) -> t.Optional[exp.Expression]:
            """Override to handle TABLE keyword inside function arguments.

            In Flink SQL, windowing TVFs use syntax like:
                TUMBLE(TABLE orders, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE)

            The TABLE keyword before a table name needs special handling.
            """
            # Check for TABLE keyword (Flink TVF syntax)
            if self._match(TokenType.TABLE):
                # Parse the table reference after TABLE keyword
                table = self._parse_table_parts()
                if table:
                    # Wrap in Anonymous to preserve the TABLE keyword context
                    return exp.Anonymous(this="TABLE", expressions=[table])
                else:
                    self._retreat(self._index - 1)

            # Fall back to parent implementation
            return super()._parse_lambda(alias=alias)

    class Generator(Generator):
        """Flink SQL generator with custom type mappings."""

        # Map sqlglot types to Flink SQL types
        TYPE_MAPPING = {
            exp.DataType.Type.VARCHAR: "STRING",
            exp.DataType.Type.TEXT: "STRING",
            exp.DataType.Type.CHAR: "STRING",
            exp.DataType.Type.NVARCHAR: "STRING",
            exp.DataType.Type.NCHAR: "STRING",
            exp.DataType.Type.VARBINARY: "BYTES",
            exp.DataType.Type.BINARY: "BYTES",
            exp.DataType.Type.TINYINT: "TINYINT",
            exp.DataType.Type.SMALLINT: "SMALLINT",
            exp.DataType.Type.INT: "INT",
            exp.DataType.Type.BIGINT: "BIGINT",
            exp.DataType.Type.FLOAT: "FLOAT",
            exp.DataType.Type.DOUBLE: "DOUBLE",
            exp.DataType.Type.DECIMAL: "DECIMAL",
            exp.DataType.Type.BOOLEAN: "BOOLEAN",
            exp.DataType.Type.DATE: "DATE",
            exp.DataType.Type.TIME: "TIME",
            exp.DataType.Type.DATETIME: "TIMESTAMP(3)",
            exp.DataType.Type.TIMESTAMP: "TIMESTAMP(3)",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMP_LTZ(3)",
            exp.DataType.Type.TIMESTAMPTZ: "TIMESTAMP WITH TIME ZONE",
        }


# Flink-specific type inference helpers
FLINK_FUNCTION_TYPES = {
    # Window time attribute functions
    "TUMBLE_START": "TIMESTAMP(3)",
    "TUMBLE_END": "TIMESTAMP(3)",
    "TUMBLE_ROWTIME": "TIMESTAMP(3)",
    "TUMBLE_PROCTIME": "TIMESTAMP_LTZ(3)",
    "HOP_START": "TIMESTAMP(3)",
    "HOP_END": "TIMESTAMP(3)",
    "HOP_ROWTIME": "TIMESTAMP(3)",
    "HOP_PROCTIME": "TIMESTAMP_LTZ(3)",
    "SESSION_START": "TIMESTAMP(3)",
    "SESSION_END": "TIMESTAMP(3)",
    "SESSION_ROWTIME": "TIMESTAMP(3)",
    "SESSION_PROCTIME": "TIMESTAMP_LTZ(3)",
    "PROCTIME": "TIMESTAMP_LTZ(3)",
    # TVF window output columns
    "window_start": "TIMESTAMP(3)",
    "window_end": "TIMESTAMP(3)",
    "window_time": "TIMESTAMP(3)",
    # Datetime functions
    "TO_TIMESTAMP_LTZ": "TIMESTAMP_LTZ(3)",
    "CURRENT_WATERMARK": "TIMESTAMP(3)",
    "SOURCE_WATERMARK": "TIMESTAMP(3)",
    "TIMESTAMPDIFF": "BIGINT",
    "CONVERT_TZ": "STRING",
    "TO_DATE": "DATE",
    "DAYOFWEEK": "INT",
    "DAYOFYEAR": "INT",
    "WEEKOFYEAR": "INT",
    "QUARTER": "INT",
    # JSON functions
    "JSON_VALUE": "STRING",
    "JSON_QUERY": "STRING",
    "JSON_EXISTS": "BOOLEAN",
    "JSON_ARRAY": "STRING",
    "JSON_OBJECT": "STRING",
    "JSON_ARRAYAGG": "STRING",
    "JSON_OBJECTAGG": "STRING",
    "JSON_STRING": "STRING",
    "JSON_QUOTE": "STRING",
    "JSON_UNQUOTE": "STRING",
    "IS_JSON": "BOOLEAN",
    # String functions
    "REGEXP_EXTRACT": "STRING",
    "REGEXP_REPLACE": "STRING",
    "REGEXP": "BOOLEAN",
    "SPLIT_INDEX": "STRING",
    "PARSE_URL": "STRING",
    "URL_ENCODE": "STRING",
    "URL_DECODE": "STRING",
    "FROM_BASE64": "BYTES",
    "TO_BASE64": "STRING",
    "INITCAP": "STRING",
    "INSTR": "INT",
    "LOCATE": "INT",
    "LPAD": "STRING",
    "RPAD": "STRING",
    "LTRIM": "STRING",
    "RTRIM": "STRING",
    "BTRIM": "STRING",
    "OVERLAY": "STRING",
    "TRANSLATE": "STRING",
    "ELT": "STRING",
    # Aggregate functions
    "LISTAGG": "STRING",
    "STDDEV_POP": "DOUBLE",
    "STDDEV_SAMP": "DOUBLE",
    "VAR_POP": "DOUBLE",
    "VAR_SAMP": "DOUBLE",
    "VARIANCE": "DOUBLE",
    "CUME_DIST": "DOUBLE",
    "PERCENT_RANK": "DOUBLE",
    # Array functions
    "CARDINALITY": "INT",
    "ARRAY_CONTAINS": "BOOLEAN",
    "ARRAY_POSITION": "INT",
    # Conditional functions
    "GREATEST": None,  # Returns type of arguments
    "LEAST": None,  # Returns type of arguments
    # Math functions
    "LOG2": "DOUBLE",
    "LOG10": "DOUBLE",
    "SINH": "DOUBLE",
    "COSH": "DOUBLE",
    "TANH": "DOUBLE",
    "COT": "DOUBLE",
    "DEGREES": "DOUBLE",
    "RADIANS": "DOUBLE",
    "RAND_INTEGER": "INT",
    # ML functions (Confluent Flink AI/ML)
    "ML_PREDICT": "ROW",
    "ML_EVALUATE": "ROW",
}

# Legacy alias for backward compatibility
FLINK_WINDOW_FUNCTIONS = FLINK_FUNCTION_TYPES


def get_flink_function_type(func_name: str) -> str | None:
    """Get the return type of a Flink-specific function.

    Args:
        func_name: The function name (case-insensitive)

    Returns:
        The Flink SQL type string, or None if not a known Flink function
    """
    return FLINK_FUNCTION_TYPES.get(func_name.upper())


def is_flink_window_function(func_name: str) -> bool:
    """Check if a function is a Flink window function.

    Args:
        func_name: The function name (case-insensitive)

    Returns:
        True if it's a Flink window function
    """
    upper_name = func_name.upper()
    return (
        upper_name.startswith("TUMBLE") or
        upper_name.startswith("HOP") or
        upper_name.startswith("SESSION") or
        upper_name in ("PROCTIME", "ROWTIME", "CUMULATE")
    )
