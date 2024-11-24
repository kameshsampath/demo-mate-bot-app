import os
import logging
import requests
from typing import Dict, Any

from utils.jwt_generator import JWTGenerator


class Cortlayst:

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(os.getenv("APP_LOG_LEVEL", logging.WARNING))

    def __init__(
        self,
        account: str,
        user: str,
        private_key_file_path: str,
        host: str,
        database: str = "slack_demo",
        schema: str = "data",
        stage: str = "semantic_models",
        file: str = "support_tickets_semantic_model.yaml",
    ):
        self.account = account
        self.user = user
        self.jwt_generator = JWTGenerator(
            account,
            user,
            private_key_file_path,
        )
        self.database = database
        self.schema = schema
        self.stage = stage
        self.file = file
        self.analyst_endpoint = f"https://{host}/api/v2/cortex/analyst/message"

    def get_token(self):
        self.LOGGER.debug("Getting JWT Token")
        return self.jwt_generator.generate_token()

    def answer(self, question) -> Dict[str, Any]:
        self.LOGGER.debug(f"Answering question:{question}")
        jwt_token = self.get_token()
        self.LOGGER.debug(f"Token:{jwt_token}")
        payload = {
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": question}],
                }
            ],
            "semantic_model_file": f"@{self.database}.{self.schema}.{self.stage}/{self.file}",
        }

        self.LOGGER.debug(f"Analyst Endpoint:{self.analyst_endpoint}")
        self.LOGGER.debug(f"Request Payload:{payload}")

        resp = requests.post(
            url=f"{self.analyst_endpoint}",
            json=payload,
            headers={
                "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {jwt_token}",
            },
        )

        request_id = resp.headers.get("X-Snowflake-Request-Id")
        if resp.status_code == 200:
            self.LOGGER.debug(f"Response:{resp.text}")
            return {**resp.json(), "request_id": request_id}
        else:
            raise Exception(
                f"Failed request (id: {request_id}) with status {resp.status_code}: {resp.text}"
            )
