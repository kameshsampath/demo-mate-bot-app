import logging
import os
import pytest
import json

from snowflake.core import Root
from snowflake.snowpark.session import Session

from handler_tasks.cortalyst import Cortlayst

logger = logging.getLogger("setup_tests")
logging.basicConfig(
    format="%(levelname)s:%(message)s",
)


@pytest.fixture
def session():
    session = Session.builder.getOrCreate()
    return session


@pytest.fixture
def cortalyst(session):
    curr_path = os.path.abspath(os.path.dirname(__file__))
    _pk_file = os.path.join(
        curr_path,
        "..",
        ".snowflake",
        "snowflake_kameshsampath.p8",
    )
    cortex_analyst = Cortlayst(
        account=session.conf.get("account"),
        user=session.conf.get("user"),
        private_key_file_path=_pk_file,
        host=session.conf.get("host"),
    )
    return cortex_analyst


class TestSetup:
    def test_answer(self, cortalyst):
        logger.debug("Answer")
        question = "Can you show me a breakdown of customer support tickets by service type - cellular vs business internet?"
        res = cortalyst.answer(question)
        assert res is not None
        assert "message" in res
        content = res["message"]["content"]
        assert content is not None
