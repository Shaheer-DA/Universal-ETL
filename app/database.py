from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect


def get_db_engine(config):
    safe_user = quote_plus(config["user"])
    safe_password = quote_plus(config["password"])

    db_url = f"mysql+pymysql://{safe_user}:{safe_password}@{config['host']}/{config['dbname']}"

    # v23.0: Removed compress=True to fix compatibility issues
    engine = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": 60, "read_timeout": 600, "write_timeout": 600},
    )
    return engine


def get_tables(engine):
    return inspect(engine).get_table_names()


def get_columns(engine, table_name):
    return [col["name"] for col in inspect(engine).get_columns(table_name)]


def get_date_columns(engine, table_name):
    return [
        col["name"]
        for col in inspect(engine).get_columns(table_name)
        if str(col["type"]).startswith(("DATETIME", "DATE", "TIMESTAMP"))
    ]
