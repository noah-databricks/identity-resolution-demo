"""Share Lakebase ownership of the steward schema between the operator and the app.

The app runs schema.sql at startup, which needs ownership of existing tables (ALTER
TABLE, CREATE INDEX). A shared NOLOGIN role owns the schema and its objects, and both
the operator and the app's service principal are members, so either can apply it.
Rerun after the app is recreated with a new service principal.

Usage: python apps/steward/regrant_app_principal.py <app-sp-client-id> [--profile P]
"""
import argparse

SCHEMA, OWNER = 'steward', 'steward_owner'


def share_ownership(conn, operator, app_principal):
    with conn.transaction():
        conn.execute(f"DO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='{OWNER}') "
                     f"THEN CREATE ROLE {OWNER} NOLOGIN; END IF; END$$")
        conn.execute(f'GRANT {OWNER} TO "{operator}"')
        conn.execute(f'GRANT {OWNER} TO "{app_principal}"')
        conn.execute(f'ALTER SCHEMA {SCHEMA} OWNER TO {OWNER}')
        for (table,) in conn.execute('SELECT tablename FROM pg_tables WHERE schemaname=%s', (SCHEMA,)).fetchall():
            conn.execute(f'ALTER TABLE {SCHEMA}."{table}" OWNER TO {OWNER}')
        for (seq,) in conn.execute('SELECT sequencename FROM pg_sequences WHERE schemaname=%s', (SCHEMA,)).fetchall():
            conn.execute(f'ALTER SEQUENCE {SCHEMA}."{seq}" OWNER TO {OWNER}')
    return conn.execute('SELECT tableowner, count(*) FROM pg_tables WHERE schemaname=%s GROUP BY 1',
                        (SCHEMA,)).fetchall()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('app_principal')
    parser.add_argument('--profile', help='Databricks CLI profile (default: ambient auth)')
    args = parser.parse_args(argv)
    import os
    from lakebase import create_operator_pool
    pool = create_operator_pool(args.profile)
    with pool, pool.connection() as conn:
        print(share_ownership(conn, os.environ['PGUSER'], args.app_principal))


if __name__ == '__main__':
    main()
