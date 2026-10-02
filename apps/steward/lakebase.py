"""Connection pool with fresh OAuth credentials on each new physical connection."""
import os
from databricks.sdk import WorkspaceClient
from psycopg_pool import ConnectionPool

OPERATOR_DEFAULTS = {
    'PGDATABASE': 'databricks_postgres',
    'LAKEBASE_ENDPOINT': 'projects/identity-steward/branches/production/endpoints/primary',
}


def create_pool() -> ConnectionPool:
    workspace = WorkspaceClient()
    endpoint = os.environ['LAKEBASE_ENDPOINT']

    import psycopg

    class OAuthConnection(psycopg.Connection):
        @classmethod
        def connect(cls, conninfo='', **kwargs):
            kwargs['password'] = workspace.postgres.generate_database_credential(endpoint=endpoint).token
            return super().connect(conninfo, **kwargs)

    return ConnectionPool(
        connection_class=OAuthConnection,
        kwargs={
            'host':os.environ['PGHOST'],
            'dbname':os.environ.get('PGDATABASE','databricks_postgres'),
            'user':os.environ.get('PGUSER') or os.environ['DATABRICKS_CLIENT_ID'],
            'sslmode':'require',
            'connect_timeout':15,
        },
        min_size=1, max_size=5, timeout=20, max_lifetime=1800,
        check=ConnectionPool.check_connection,
        open=False,
    )


def create_operator_pool(profile: str | None = None) -> ConnectionPool:
    """Pool for operator scripts and setup tasks: the caller connects as their own Lakebase role.

    With no profile, uses ambient Databricks auth (a job task or DATABRICKS_* variables).
    """
    if profile:
        os.environ['DATABRICKS_CONFIG_PROFILE'] = profile
    for key, value in OPERATOR_DEFAULTS.items():
        os.environ.setdefault(key, value)
    workspace = WorkspaceClient()
    if not os.environ.get('PGHOST'):
        endpoint = workspace.postgres.get_endpoint(name=os.environ['LAKEBASE_ENDPOINT'])
        os.environ['PGHOST'] = endpoint.status.hosts.host
    if not os.environ.get('PGUSER'):
        os.environ['PGUSER'] = workspace.current_user.me().user_name
    return create_pool()
