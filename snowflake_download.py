#!/usr/bin/env python3
"""CLI tool to download Snowflake query results to CSV using dbt profiles.yml."""

import argparse
import os
import sys

import pandas as pd
import yaml
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import snowflake.connector

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PROFILES_PATH = os.path.join(SCRIPT_DIR, "profiles.yml")


def load_profile(profiles_path, profile_name):
    """Load a dbt profile from profiles.yml and return the dev output config."""
    try:
        with open(profiles_path) as f:
            profiles = yaml.safe_load(f)
    except FileNotFoundError:
        sys.exit(f"Error: Profiles file not found: {profiles_path}")

    if profile_name not in profiles:
        available = ", ".join(profiles.keys())
        sys.exit(f"Error: Profile '{profile_name}' not found. Available: {available}")

    profile = profiles[profile_name]
    target = profile.get("target", "dev")
    outputs = profile.get("outputs", {})

    if target not in outputs:
        sys.exit(f"Error: Target '{target}' not found in profile '{profile_name}'.")

    return outputs[target]


def get_connection_params(config):
    """Build Snowflake connection params from a dbt profile config."""
    private_key_path = os.path.expanduser(config["private_key_path"])
    passphrase = config.get("private_key_passphrase", "").encode()

    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase or None,
            backend=default_backend(),
        )

    pk_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    params = {
        "account": config["account"],
        "user": config["user"],
        "private_key": pk_bytes,
    }

    for key in ("role", "warehouse", "database", "schema"):
        if key in config:
            params[key] = config[key]

    return params


def run_query(params, query):
    """Connect to Snowflake, execute query, and return a DataFrame."""
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**params)
    try:
        print("Running query...")
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        return df
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Download Snowflake query results to CSV."
    )
    query_group = parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument("-q", "--query", help="Inline SQL query to execute")
    query_group.add_argument("-f", "--file", help="Path to a .sql file containing the query")
    parser.add_argument(
        "-o", "--output", default="output.csv", help="Output CSV file path (default: output.csv)"
    )
    parser.add_argument(
        "-p", "--profile", default="snowflake-data-analytics",
        help="dbt profile name (default: snowflake-data-analytics)",
    )
    parser.add_argument(
        "--profiles-path", default=DEFAULT_PROFILES_PATH,
        help=f"Path to profiles.yml (default: {DEFAULT_PROFILES_PATH})",
    )

    args = parser.parse_args()

    if args.file:
        try:
            with open(args.file) as f:
                query = f.read().strip()
        except FileNotFoundError:
            sys.exit(f"Error: SQL file not found: {args.file}")
        if not query:
            sys.exit(f"Error: SQL file is empty: {args.file}")
    else:
        query = args.query

    config = load_profile(args.profiles_path, args.profile)
    params = get_connection_params(config)
    df = run_query(params, query)
    df.to_csv(args.output, index=False)
    print(f"Done. {len(df)} rows written to {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
