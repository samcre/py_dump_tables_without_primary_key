#!/usr/bin/env python3

__author__ = "Samuel Crespo"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
from logzero import logger
import os
import psycopg2
from sh import pg_dump, psql, pg_restore

TMP_WORKDIR = "/tmp"
SELECT_WO_PK = """
select tab.table_schema,
       tab.table_name
from information_schema.tables tab
left join information_schema.table_constraints tco
          on tab.table_schema = tco.table_schema
          and tab.table_name = tco.table_name
          and tco.constraint_type = 'PRIMARY KEY'
where tab.table_type = 'BASE TABLE'
      and tab.table_schema not in ('pg_catalog',
                                   'information_schema',
                                   'pglogical')
      and tco.constraint_name is null
order by table_schema,
         table_name;
"""


def get_tables_wo_pk(connection_string):
    """
    returns a list with format $schema.$table without primary keys
    """
    result = []
    with psycopg2.connect(connection_string).cursor() as sql:
        sql.execute(SELECT_WO_PK)
        for i in sql.fetchall():
            result.append(f"{'.'.join(i)}")
    return result


def dump_table(connection_string, table, file=None):
    if file is None:
        file = f'{TMP_WORKDIR}/{table}.sql'
    try:
        logger.info(f'Dumping table {table} on to {file}…')
        with open(file, 'wb') as f:
            pg_dump(
                "-d", connection_string,
                "--table", table,
                "--no-owner",
                "--no-privileges",
                "-f", file
            )
        logger.info(f'Finished {table} dump.')
    except Exception as e:
        logger.error(f'Error occurred during dump: {e}')
        exit(1)


def restore_table(connection_string, table, file=None):
    if file is None:
        file = f'{TMP_WORKDIR}/{table}.sql'
    try:
        logger.info(f'Restoring table {table} on target database…')
        with open(file) as f:
            psql(
                 "-d", connection_string,
                 f"--command=TRUNCATE TABLE {table};",)
            psql(
                "-d", connection_string,
                "-f", file
            )
            # pg_restore(
            #            "-d", connection_string,
            #            "--clean",
            #            "--table", table,
            #            "-1"
            #            )
        logger.info(f'Finished {table} restore.')
    except Exception as e:
        logger.error(f'Error occurred during restore: {e}')
        exit(1)


def get_connection_string(**kargs):
    result = None
    for key, value in kargs.items():
        if result is None:
            result = f"{key}={value}"
        else:
            result = f"{result} {key}={value}"
    return result


def create_pgsql_flags(prefix, p):
    p.add_argument(
        f"--{prefix}-host",
        action="store",
        help=f"{prefix} database server host (default: \"localhost\""
    )
    p.add_argument(
        f"--{prefix}-port",
        action="store",
        default="5432",
        help=f"{prefix} database server port (default: \"5432\")"
    )
    p.add_argument(
        f"--{prefix}-username",
        action="store",
        default="postgres",
        help=f"{prefix} database user name (default: \"postgres\")"
    )
    p.add_argument(
        f"--{prefix}-password",
        action="store",
        default=p.get_default(f'{prefix}_username'),
        help=f"{prefix} database user password "
        f"(default: \"{p.get_default(f'{prefix}_username')}\")"
    )


def main(opts):
    logger.info(opts)
    try:
        for database in opts.databases.split(','):
            source_cs = get_connection_string(
                host=opts.source_host,
                port=opts.source_port,
                user=opts.source_username,
                password=opts.source_password,
                dbname=database
            )
            target_cs = get_connection_string(
                host=opts.target_host,
                port=opts.target_port,
                user=opts.target_username,
                password=opts.target_password,
                dbname=database
            )
            tables = get_tables_wo_pk(source_cs)
            logger.info(f"Found {len(tables)} tables without "
                        f"Primary Key on database {database}")
            for table in tables:
                file = f'{TMP_WORKDIR}/{table}.sql'
                dump_table(source_cs, table, file)
                restore_table(target_cs, table, file)
                os.remove(file)
    except Exception as e:
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    create_pgsql_flags("source", parser)
    create_pgsql_flags("target", parser)
    parser.add_argument(
        "--databases",
        action="store",
        help='comma-separated databases where to find tables without PK.'
    )
    args = parser.parse_args()
    main(args)
