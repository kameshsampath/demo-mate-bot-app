from typing import Optional
import os
from datetime import datetime, timezone, timedelta

import logging
from snowflake.core import Root, CreateMode
from snowflake.core.database import Database
from snowflake.core.schema import Schema
from snowflake.core.table import Table, TableColumn
from snowflake.core.stage import Stage, StageEncryption, StageDirectoryTable
from snowflake.core.pipe import Pipe


class DBSetup:

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.DEBUG)
    _mode = CreateMode.if_not_exists

    def __init__(self, session):
        self.session = session
        self.root = Root(session)

    def create_db(self, db_name: str) -> None:
        """
        Create the database that will be used in the demo
        Args:
            db_name - the name of the database to create
        """

        self.LOGGER.debug(f"Creating database {db_name}")
        database = Database(db_name, comment="created by slack bot setup")
        try:
            self.root.databases[db_name].create_or_alter(database)
        except Exception as e:
            self.LOGGER.error(e)
            raise f"Error creating database {db_name},{e}"

    def create_schema(self, schema_name: str, db_name: Database) -> None:
        """
        Create the Schema for the demo
        """
        self.LOGGER.debug(f"Creating Schema {schema_name}")
        schema = Schema(schema_name, comment="created by slack bot setup")
        try:

            self.root.databases[db_name].schemas.create(
                schema=schema,
                mode=self._mode,
            )
        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating schema {schema_name},{e}")

    def create_file_formats(
        self,
        db_name: str,
        schema_name: str,
    ):
        """
        Create the CSV File Format used for data loading
        """
        try:
            ff_name = ".".join([db_name, schema_name, "csvformat"])
            self.LOGGER.debug(f"Creating file format {ff_name}")
            df = self.session.sql(
                f"""CREATE OR REPLACE FILE FORMAT {ff_name}
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TYPE = 'CSV'
COMMENT = 'created by slack bot setup';
"""
            )
            df.collect()
        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating file format {ff_name},{e}")

    def create_stage(
        self,
        db_name: str,
        schema_name: str,
        stage_name: str = "support_tickets_data",
    ) -> None:
        """
        Create the stage used for the demo
        """
        try:
            stages = [
                Stage(
                    name=stage_name,
                    url="s3://sfquickstarts/finetuning_llm_using_snowflake_cortex_ai/",
                    comment="created by slack bot setup",
                    directory_table=StageDirectoryTable(enable=True),
                ),
                Stage(
                    name=f"older_than_7days_{stage_name}",
                    encryption=StageEncryption(type="SNOWFLAKE_SSE"),
                    directory_table=StageDirectoryTable(enable=True),
                    comment="created by slack bot setup",
                ),
                Stage(
                    name="semantic_models",
                    encryption=StageEncryption(type="SNOWFLAKE_SSE"),
                    directory_table=StageDirectoryTable(enable=True),
                    comment="created by slack bot setup",
                ),
            ]
            for stage in stages:
                (
                    self.root.databases[db_name]
                    .schemas[schema_name]
                    .stages.create(
                        stage,
                        mode=self._mode,
                    )
                )
            # upload the semantic model file
            curr_path = os.path.abspath(os.path.dirname(__file__))
            _model_file = os.path.join(
                curr_path,
                "..",
                "data",
                "support_tickets_semantic_model.yaml",
            )
            self.LOGGER.debug(
                f"Uploading semantic model {_model_file} to stage 'semantic_models'"
            )
            self.root.databases[db_name].schemas[schema_name].stages[
                "semantic_models"
            ].put(
                _model_file,
                stage_location="/",
                auto_compress=False,
                overwrite=True,
            )
        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating stages,{e}")

    def create_table(
        self,
        db_name: str,
        schema_name: str,
        table_name: str = "support_tickets",
    ) -> None:
        """
        Create the table that will be used in the demo
        """
        try:
            table_columns = [
                TableColumn(
                    name="ticket_id",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="customer_name",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="customer_email",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="service_type",
                    datatype="varchar(60)",
                ),
                TableColumn(
                    name="request",
                    datatype="varchar",
                ),
                TableColumn(
                    name="contact_preference",
                    datatype="varchar(60)",
                ),
            ]
            table = Table(
                name=table_name,
                columns=table_columns,
                comment="created by slack bot setup",
            )
            (
                self.root.databases[db_name]
                .schemas[schema_name]
                .tables.create(
                    table,
                    mode=self._mode,
                )
            )
        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating table {table_name},{e}")

    def is_date_older_than_7days(self, date_str):
        """
        check if a date is older than 7 days
        """
        # Parse the date string into datetime object
        date_obj = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")

        # Get current time in UTC
        current_time = datetime.now(timezone.utc)

        # Calculate the difference
        time_difference = current_time - date_obj.replace(tzinfo=timezone.utc)

        # Check if difference is more than 7 days
        return time_difference > timedelta(days=7)

    def pipe_and_load(
        self,
        db_name: str,
        schema_name: str,
        stage_name: str = "support_tickets_data",
        table_name: str = "support_tickets",
        ff_name: str = "csvformat",
        pipe_name: str = "support_tickets_data",
    ):
        """
        Create Pipe and load the data from the external stage
        """
        try:
            self.LOGGER.debug("Pipe and Load")

            _table_fqn = f"{db_name}.{schema_name}.{table_name}"
            _stage_fqn = f"{db_name}.{schema_name}.{stage_name}"
            _target_stage_fqn = f"{db_name}.{schema_name}.older_than_7days_{stage_name}"
            ff_fqn = f"{db_name}.{schema_name}.{ff_name}"

            support_tickets_pipes = [
                Pipe(
                    name=pipe_name,
                    auto_ingest=True,
                    comment="created by slack bot setup",
                    copy_statement=f"COPY INTO {_table_fqn} FROM @{_stage_fqn}/ FILE_FORMAT = (FORMAT_NAME = '{ff_fqn}')",
                ),
                Pipe(
                    name=f"older_than_7days_{pipe_name}",
                    comment="created by slack bot setup to ingest 7 days older data files from external stage",
                    copy_statement=f"COPY INTO {_table_fqn} FROM @{_target_stage_fqn} FILE_FORMAT = (FORMAT_NAME = '{ff_fqn}')",
                ),
            ]

            pipes = self.root.databases[db_name].schemas[schema_name].pipes

            for pipe in support_tickets_pipes:
                pipes.create(
                    pipe,
                    mode=self._mode,
                )
            # handle files older than 7 days (!!!IMPORTANT!!! Only for Demos)
            self.LOGGER.debug("Handle files greater than 7 days, just for demo.")
            old_stage_files = (
                self.root.databases[db_name]
                .schemas[schema_name]
                .stages[stage_name]
                .list_files(pattern=".*[.csv]")
            )
            older_than_7days = [
                os.path.basename(f.name)
                for f in old_stage_files
                if self.is_date_older_than_7days(f.last_modified)
            ]

            if len(older_than_7days) > 0:
                _older_files = ",".join(f"'{x}'" for x in older_than_7days)
                self.LOGGER.debug(
                    f"Files Older than 7 days to be loaded to internal stage {_older_files}"
                )
                self.session.sql(
                    f"""
                COPY FILES 
                INTO @{_target_stage_fqn}
                FROM @{_stage_fqn}
                FILES=({_older_files})
                    """
                ).collect()
                # trigger run
                _pipe = (
                    self.root.databases[db_name]
                    .schemas[schema_name]
                    .pipes[f"older_than_7days_{pipe_name}"]
                )
                _pipe.refresh()

        except Exception as e:
            self.LOGGER.error(e)
            raise Exception(f"Error creating pipe and loading data,{e}")

    def do(
        self,
        db_name: str = "demo_db",
        schema_name: str = "data",
    ):
        """
        Creates or alters Snowflake Database objects using Snowflake Python API.
        """
        try:
            self.LOGGER.debug(f"Using Database : {db_name} and Schema : {schema_name}")

            self.create_db(db_name)
            self.create_schema(
                schema_name=schema_name,
                db_name=db_name,
            )
            self.create_file_formats(
                db_name=db_name,
                schema_name=schema_name,
            )
            self.create_stage(
                db_name=db_name,
                schema_name=schema_name,
            )
            self.create_table(
                db_name=db_name,
                schema_name=schema_name,
            )
            self.pipe_and_load(
                db_name=db_name,
                schema_name=schema_name,
            )
            self.LOGGER.info("Setup successful")
        except Exception as e:
            self.LOGGER.error(
                "Error setting up demo",
                exc_info=True,
            )
            raise Exception(f"Error setting up demo,{e}")
