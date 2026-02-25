"""
Database helpers — connection factory and statement executor.
"""
import logging

import pymysql
import sqlalchemy as sa

from lending_report.config import DB_CONFIG

log = logging.getLogger(__name__)


def get_engine():
    """Create a SQLAlchemy engine from DB_CONFIG."""
    pymysql.install_as_MySQLdb()
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return sa.create_engine(url, pool_recycle=1800, pool_pre_ping=True)


def execute_statements(statements, engine=None):
    """
    Execute a list of SQL statements inside a single transaction.

    Each element in *statements* is one complete SQL string (no trailing
    semicolon required).  Statements run sequentially; the whole batch
    is committed at the end or rolled back on error.
    """
    engine = engine or get_engine()
    with engine.begin() as conn:
        for stmt in statements:
            trimmed = stmt.strip()
            if not trimmed:
                continue
            log.info("SQL  ▸ %s", trimmed[:120])
            conn.execute(sa.text(trimmed))
