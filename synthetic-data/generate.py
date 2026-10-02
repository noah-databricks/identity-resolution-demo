"""Thin Spark SQL runner for the blinded identity-resolution demo generator."""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from pathlib import Path


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Committed public reference data (see reference/README.md). Column types are explicit
# so the loaded Delta tables are identical on every run.
REFERENCE_TABLES: dict[str, dict[str, str]] = {
    "au_localities": {"locality_key": "string", "suburb": "string", "state": "string", "postcode": "string",
                      "sa4": "string", "apartment_pct": "int"},
    "home_locality_weights": {"culture": "string", "locality_key": "string", "weight": "double", "lo": "bigint",
                              "hi": "bigint"},
    "overseas_localities": {"overseas_key": "string", "country": "string", "city": "string", "suburb": "string",
                            "region": "string", "postcode": "string", "culture": "string", "phone_cc": "string",
                            "streets": "string", "weight": "double", "lo": "bigint", "hi": "bigint"},
    "cultures": {"culture": "string", "target_pct": "double", "anglicised_pct_pre1975": "int",
                 "anglicised_pct_post1975": "int", "weight": "double", "lo": "bigint", "hi": "bigint"},
    "given_names": {"culture": "string", "sex": "string", "decade": "int", "name": "string", "weight": "double",
                    "lo": "bigint", "hi": "bigint"},
    "family_names": {"culture": "string", "name": "string", "weight": "double", "lo": "bigint", "hi": "bigint"},
    "street_names": {"street_name": "string", "has_type": "boolean", "weight": "double", "lo": "bigint",
                     "hi": "bigint"},
    "street_types": {"street_type": "string", "weight": "double", "lo": "bigint", "hi": "bigint"},
    "employers": {"employer_key": "bigint", "company_name": "string", "domain": "string", "email_pattern": "string",
                  "office_line1": "string", "office_suburb": "string", "office_state": "string",
                  "office_postcode": "string", "area_code": "string", "weight": "double", "lo": "bigint",
                  "hi": "bigint"},
    "email_domains": {"culture": "string", "age_band": "string", "domain": "string", "weight": "double",
                      "lo": "bigint", "hi": "bigint"},
    "nicknames": {"formal_name": "string", "nickname": "string"},
    "twin_names": {"pair_key": "bigint", "first_name": "string", "second_name": "string", "sex_rule": "string"},
}
STAGES = {"generate": "01_generate.sql", "validate": "02_validate_realism.sql"}


def _identifier(value: str, flag: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"{flag} must be a simple Unity Catalog identifier")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=os.getenv("DEMO_CATALOG"))
    parser.add_argument("--source-schema", default=os.getenv("DEMO_SOURCE_SCHEMA", "source"))
    parser.add_argument("--truth-schema", default=os.getenv("DEMO_TRUTH_SCHEMA", "truth"))
    parser.add_argument("--reference-schema", default=os.getenv("DEMO_REFERENCE_SCHEMA", "reference"))
    parser.add_argument("--seed", default=os.getenv("DEMO_GENERATOR_SEED", "identity-demo-v1"))
    parser.add_argument("--generator-version", default=os.getenv("DEMO_GENERATOR_VERSION", "4.1.0"))
    parser.add_argument("--people", type=int, default=int(os.getenv("DEMO_PEOPLE", "61000")))
    parser.add_argument("--pos-rows", type=int, default=int(os.getenv("DEMO_POS_ROWS", "500000")))
    parser.add_argument("--stage", choices=sorted(STAGES), default="generate")
    args = parser.parse_args()
    if not args.catalog:
        parser.error("--catalog (or DEMO_CATALOG) is required")
    if args.people < 4000:
        parser.error("--people must be at least 4000 to retain curated scenarios")
    if args.pos_rows < args.people:
        parser.error("--pos-rows must be at least the population size")
    args.catalog = _identifier(args.catalog, "--catalog")
    args.source_schema = _identifier(args.source_schema, "--source-schema")
    args.truth_schema = _identifier(args.truth_schema, "--truth-schema")
    args.reference_schema = _identifier(args.reference_schema, "--reference-schema")
    return args


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def render_sql(template: str, args: argparse.Namespace) -> str:
    values = {
        "catalog": args.catalog,
        "source_schema": args.source_schema,
        "truth_schema": args.truth_schema,
        "reference_schema": getattr(args, "reference_schema", "reference"),
        "seed": _sql_literal(args.seed),
        "generator_version": _sql_literal(args.generator_version),
        "people": str(args.people),
        "households": str((args.people + 1) // 2),
        "pos_rows": str(args.pos_rows),
    }
    for key, value in values.items():
        template = template.replace("{{" + key + "}}", value)
    unresolved = re.findall(r"\{\{[^}]+\}\}", template)
    if unresolved:
        raise ValueError(f"unresolved SQL template values: {unresolved}")
    return template


_CASTS = {"int": int, "bigint": int, "double": float, "string": str,
          "boolean": lambda v: v.strip().lower() == "true"}


def read_reference(path: Path, columns: dict[str, str]) -> list[tuple]:
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = set(columns) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{path.name} is missing columns {sorted(missing)}")
        return [tuple(_CASTS[t](row[c]) if row[c] != "" or t == "string" else None for c, t in columns.items())
                for row in reader]


def load_reference_tables(spark, reference_dir: Path, args: argparse.Namespace) -> None:
    target = f"{args.catalog}.{args.reference_schema}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target} COMMENT "
              "'Public reference data for the synthetic generator: gazetteer, name frequencies, fictional employers. "
              "Contains no synthetic people or benchmark truth.'")
    for table, columns in REFERENCE_TABLES.items():
        rows = read_reference(reference_dir / f"{table}.csv", columns)
        schema = ", ".join(f"`{c}` {t}" for c, t in columns.items())
        (spark.createDataFrame(rows, schema=schema).write.mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(f"{target}.{table}"))


def main() -> None:
    args = parse_args()
    # Serverless Spark Python tasks execute the file through a notebook wrapper,
    # where __file__ is intentionally absent but argv[0] remains the script path.
    script_path = Path(globals().get("__file__", sys.argv[0])).resolve()
    sql_path = script_path.with_name("sql") / STAGES[args.stage]
    rendered = render_sql(sql_path.read_text(encoding="utf-8"), args)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    if args.stage == "generate":
        load_reference_tables(spark, script_path.with_name("reference"), args)
    for statement in (s.strip() for s in rendered.split("-- COMMAND ----------")):
        if statement:
            body = "\n".join(l for l in statement.splitlines() if not l.lstrip().startswith("--")).lstrip()
            if not body:
                continue
            if body.upper().startswith(("SELECT", "WITH")):
                # Queries are lazy; collect them so every assert_true actually executes.
                for row in spark.sql(statement).collect():
                    print(row.asDict())
            else:
                spark.sql(statement)


if __name__ == "__main__":
    main()
