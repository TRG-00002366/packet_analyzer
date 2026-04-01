import os


def build_snowflake_options(table_name=None):
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    url = os.getenv("SNOWFLAKE_URL")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    role = os.getenv("SNOWFLAKE_ROLE")

    if not url and account:
        url = f"{account}.snowflakecomputing.com"

    required = {
        "SNOWFLAKE_URL": url,
        "SNOWFLAKE_USER": user,
        "SNOWFLAKE_PASSWORD": password,
        "SNOWFLAKE_WAREHOUSE": warehouse,
        "SNOWFLAKE_DATABASE": database,
        "SNOWFLAKE_SCHEMA": schema,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(
            "Missing Snowflake configuration: " + ", ".join(sorted(missing))
        )

    options = {
        "sfURL": url,
        "sfUser": user,
        "sfPassword": password,
        "sfWarehouse": warehouse,
        "sfDatabase": database,
        "sfSchema": schema,
    }
    if role:
        options["sfRole"] = role
    if table_name:
        options["dbtable"] = table_name

    return options