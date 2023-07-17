"""
A cli to populate a local database with production
data. Expects a companion file called config.py to exist
with the following structure:

```python
# tables.py

# any tables that should be downloaded in their entirety
small_tables = [
    "people",
    "dogs",
    "cats"
]

# tables that must be subsampled
large_tables = {
    "events": 0.001  # the sampling percentage
}

# any constraints that must exist for the app to work
alterations = [
    "ALTER TABLE ONLY public.dogs ADD CONSTRAINT ..."
]
```

The cli can be run as follows:

# to recreate the database
> python -m copypg full 

# to just refresh the data
> python -m copypg reload
"""

import os
import re
from subprocess import run, PIPE
from textwrap import dedent

import fire

from config import alterations, small_tables, large_tables, shop_ids

all_tables = small_tables + list(large_tables.keys())
local_prod_db = "local_prod"
prod_db = os.getenv("PG_READONLY")


def run_silently(cmd):
    run(cmd, shell=True, stdout=PIPE, stderr=PIPE)


def reset_db() -> None:
    cmd = f"dropdb {local_prod_db}; createdb {local_prod_db}"
    run_silently(cmd)


def download_schema() -> None:
    opts = "".join([f" -t {t}" for t in all_tables])
    cmd = f"""
    pg_dump {prod_db} -s {opts} -f raw/prod.schema.sql
    """
    run_silently(cmd)


def download_data_for_small_tables() -> None:
    opts = "".join([f" -t {t}" for t in small_tables])
    cmd = f"""
    pg_dump {prod_db} -a {opts} -f raw/prod.data.sql
    """
    run_silently(cmd)


def download_sample_of_data_for_large_tables() -> None:
    cmd = f"""
    psql {prod_db} -c "\copy (
        select * from {{table}} 
        tablesample system ({{sample}})
        where shop_id in {('-hack-',) + shop_ids}
        )
        to 'raw/{{table}}_{{sample}}.csv' with header csv"
    """
    for table, sample in large_tables.items():
        run_silently(cmd.format(table=table, sample=sample))


def download_shop_specific_data_for_large_tables() -> None:
    # hack: python tuples are interpolated to valid sql except if they're length 1
    # so we make them always at least length 2
    cmd = f"""
    psql {prod_db} -c "\copy (
        select * from {{table}} 
        where shop_id in {('-hack-',) + shop_ids}
        )
        to 'raw/{{table}}_{{sample}}.csv' with header csv"
    """

    for table, sample in large_tables.items():
        run_silently(cmd.format(table=table, sample=sample))


def load_data_for_small_tables() -> None:
    cmd = f"psql {local_prod_db} -f raw/prod.data.sql"
    run_silently(cmd)


def load_data_for_large_tables() -> None:
    cmd = f"""
    psql {local_prod_db} -c "\copy {{table}} from 'raw/{{table}}_{{sample}}.csv' with csv header"
    """
    for table, sample in large_tables.items():
        run(cmd.format(table=table, sample=sample), shell=True)



def drop_tables() -> None:
    """Tables with problematic constraints make
    inserting data more complicated, so we drop
    and recreate them."""

    cmd = f"psql {local_prod_db} -c 'drop table if exists {{table}} cascade'"
    for table in all_tables:
        run_silently(cmd.format(table=table))

def truncate_large_tables() -> None:
    """When we want to reload large tables for specific shops, we drop the 
    existing data so there's no duplication"""

    cmd = f"psql {local_prod_db} -c 'truncate {{table}}'"
    for table in large_tables:
        run_silently(cmd.format(table=table))


def process_schema() -> None:
    """
    Eeek hacky! Some tables have constraints that make
    them difficult to copy locally. Here we strip out
    anything that makes loading data fail.
    """

    with open("raw/prod.schema.sql") as f:
        text = f.read()
        processed = re.findall("CREATE TABLE .*?;\n", text, re.DOTALL)
        processed = [
            re.sub("bigint DEFAULT nextval.*,", "serial,", t, re.MULTILINE)
            for t in processed
        ]
        processed += alterations
        with open("processed/prod.schema.sql", "w") as g:
            g.write("\n".join(processed))


def create_tables() -> None:
    cmd = f"psql {local_prod_db} -f processed/prod.schema.sql"
    run_silently(cmd)

def full():
    "Run each of the steps required to have a working local db"
    print("recreating database", local_prod_db, end="", flush=True)
    reset_db()
    print("...done")
    reload()

def reload():
    "Run each of the steps required to have a working"
    for step in dedent(
        """
    download_schema
    process_schema
    drop_tables
    create_tables
    download_data_for_small_tables
    download_sample_of_data_for_large_tables
    load_data_for_small_tables
    load_data_for_large_tables
    """
    ).split():
        print(step, end="...", flush=True)
        globals()[step]()
        print("...done")


def reload_for_shops():
    "Run each of the steps required to have a working"
    for step in dedent(
        """
    truncate_large_tables
    download_shop_specific_data_for_large_tables
    load_data_for_large_tables
    """
    ).split():
        print(step, end="...", flush=True)
        globals()[step]()
        print("...done")


if __name__ == "__main__":
    fire.Fire()
