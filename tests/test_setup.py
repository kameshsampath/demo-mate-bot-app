import os
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from snowflake.core import Root
from snowflake.snowpark.session import Session

from handler_tasks.db_setup import DBSetup

logger = logging.getLogger("setup_tests")
logging.basicConfig(
    format="%(levelname)s:%(message)s",
)


@pytest.fixture
def session():
    session = Session.builder.getOrCreate()
    return session


@pytest.fixture
def root(session):
    root = Root(session)
    return root


@pytest.fixture
def db_setup(session):
    db_setup = DBSetup(session=session)
    return db_setup


@pytest.fixture
def db_name(root):
    _db_name = "test_db"
    yield _db_name
    root.databases[_db_name].drop(if_exists=True)
    files = list(Path("../data").glob("*.yaml"))
    for file_path in files:
        file_path.unlink()
        logger.debug(f"Removed: {file_path}")


class TestSetup:
    def test_create_db(self, db_setup, root):
        want = ("test_db", "created by slack bot setup")

        db_setup.db_name = want[0]

        db_setup.create_db(want[0])

        got = root.databases[want[0]].fetch()

        assert got is not None
        assert want[0] == got.name.lower()
        assert want[1] == got.comment

    def test_create_schema(self, db_setup, root: Root):
        db_name = "test_db"
        want = ("data", "created by slack bot setup")
        db_setup.schema_name = want[0]

        db_setup.create_schema(
            want[0],
            db_name=db_name,
        )

        got = root.databases[db_name].schemas[want[0]].fetch()

        assert got is not None
        assert want[0] == got.name.lower()
        assert want[1] == got.comment

    def test_create_file_formats(self, db_setup, session: Session):
        db_name = "test_db"
        schema_name = "data"
        want = "CSV"

        db_setup.create_file_formats(
            db_name,
            schema_name,
        )

        rows = session.sql(
            f"desc file format {db_name}.{schema_name}.csvformat"
        ).collect()
        got = rows[0].as_dict()

        logger.debug(f"File Format:{got}")

        assert got is not None
        assert want == got["property_value"]
        assert want == got["property_default"]

    def test_create_stages(self, db_setup, root: Root):
        db_name = "test_db"
        schema_name = "data"
        want = (
            "support_tickets_data",
            "semantic_models",
            "older_than_7days_support_tickets_data",
            "created by slack bot setup",
        )

        db_setup.create_stage(db_name, schema_name)

        got_stages = root.databases[db_name].schemas[schema_name].stages

        assert got_stages is not None

        for got in got_stages:
            assert got is not None
            assert got.name.lower() in [want[0], want[1]]
            assert want[2] == got.comment

        stage_files = list(
            root.databases[db_name]
            .schemas[schema_name]
            .stages[want[1]]
            .list_files(pattern=".*.yaml")
        )

        logger.debug(f"Stage files{stage_files}")
        assert len(stage_files) == 1
        assert (
            "semantic_models/support_tickets_semantic_model.yaml" == stage_files[0].name
        )

    def test_create_table(self, db_setup, root: Root):
        db_name = "test_db"
        schema_name = "data"
        want = ("support_tickets", "created by slack bot setup")

        db_setup.create_table(
            db_name=db_name,
            schema_name=schema_name,
        )

        got = root.databases[db_name].schemas[schema_name].tables[want[0]].fetch()

        assert got is not None
        assert len(got.columns) == 6
        for col in got.columns:
            assert col.name in [
                "ticket_id".upper(),
                "customer_name".upper(),
                "customer_email".upper(),
                "service_type".upper(),
                "request".upper(),
                "contact_preference".upper(),
            ]
            assert col.datatype in ["VARCHAR(60)", "VARCHAR(16777216)"]
        assert want[0] == got.name.lower()
        assert want[1] == got.comment

    def test_pipe_and_load(self, db_name, db_setup, root: Root):
        schema_name = "data"
        table_name = "support_tickets"
        want = (
            [
                "support_tickets_data",
                "older_than_7days_support_tickets_data",
            ],
            [
                "created by slack bot setup",
                "created by slack bot setup to ingest 7 days older data files from external stage",
            ],
        )
        db_setup.pipe_and_load(
            db_name,
            schema_name,
        )

        for pipe_name in want[0]:
            got = root.databases[db_name].schemas[schema_name].pipes[pipe_name].fetch()
            assert got is not None
            assert pipe_name == got.name.lower()
            assert got.comment in want[1]

        # wait and check if the data is loaded
        table_fqn = f"{db_name}.{schema_name}.{table_name}"
        seconds = int(os.getenv("SETUP_TIMEOUT_SECS", 30))
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=seconds)
        _query = root.session.sql(f"select * from {table_fqn}")
        while datetime.now() < end_time:
            got = _query.count()
            elapsed_seconds = (datetime.now() - start_time).total_seconds()

            # If table is refreshed then break the loop
            if got > 0:
                logger.debug(f"Got table refreshed in {elapsed_seconds:.2f} seconds")
                break

            # Add small delay between checks
            time.sleep(1)

        if got == 0:
            logger.warning(
                f"Timeout after {elapsed_seconds:.2f} seconds without finding records"
            )
        assert got >= 151
