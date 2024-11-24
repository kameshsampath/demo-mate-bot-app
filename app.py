import os
import logging
import sys
import json
from typing import Any, Dict, List
import altair as alt
import io

from snowflake.snowpark.session import Session
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient

from handler_tasks.db_setup import DBSetup
from handler_tasks.cortalyst import Cortlayst
import handler_tasks.blocks as blocks

_log_level = os.getenv("APP_LOG_LEVEL", "WARNING")

logging.basicConfig(
    level=logging.WARNING,
    format="%(name)s:%(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger("demo_mate_bot")
logger.setLevel(level=_log_level)

try:
    session = Session.builder.getOrCreate()
except Exception as e:
    logger.error(f"Error establishing connection,{e}", exc_info=True)
    sys.exit(1)

# Initializes your app with your bot token and socket mode handler
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

db_setup: DBSetup = DBSetup(session=session)

if os.path.exists(".dbinfo"):
    logger.debug("Loading db and schema info from file .dbinfo")
    with open(".dbinfo", "r") as file:
        db_info = json.load(file)
        db_setup.db_name = db_info["db_name"]
        db_setup.schema_name = db_info["schema_name"]
        logger.debug(
            f"App will use DB: '{db_setup.db_name}' and Schema: '{ db_setup.schema_name}'"
        )


def setLogLevel(logger):
    """
    Set the logger level to APP_LOG_LEVEL env
    """
    logger.setLevel(_log_level)


def do_setup(
    client, channel_id, logger, db_name: str = "demo_db", schema_name: str = "data"
):
    """
    Calls the utility to setup the demo database and other objects
    """
    logger.debug("DO SETUP")
    try:
        client.chat_postMessage(
            channel=channel_id,
            text=f"Wait for few seconds for the setup to be done :timer_clock:",
        )

        global db_setup
        db_setup.db_name = db_name
        db_setup.schema_name = schema_name
        ## write to file for persistence
        with open(".dbinfo", "w") as file:
            json.dump(
                {"db_name": db_name, "schema_name": schema_name}, file, indent=2.0
            )
        db_setup.do()

        # Send a message with the input value
        client.chat_postMessage(
            channel=channel_id,
            text=f"""
*Congratulations!!* Demo setup successful :tada:.

Try this query in *Snowsight* to view the loaded data:  
```
SELECT * FROM {db_name}.{schema_name}.SUPPORT_TICKETS;
```""",
        )
    except Exception as e:
        logger.error(f"Error handling submission: {str(e)}")
        client.chat_postMessage(
            channel=channel_id,
            text=f"Sorry, error setting up database.{e}",
        )


@app.command("/setup")
def setup_handler(ack, client, command, respond):
    try:
        setLogLevel(logger)
        ack()
        command_text = command.get("text", "").strip()
        logger.debug(f"command_text:{command_text}")
        if not command_text:
            logger.debug("Sending Block")
            try:
                # Send the response with input block
                respond(
                    blocks=blocks.db_schema_setup,
                    response_type="ephemeral",  # Only visible to the user who triggered the command
                )
            except Exception as e:
                logger.error(f"Failed to send response: {e}")
                # Fallback response
                respond(
                    text="Sorry, there was an error displaying the setup form.",
                    response_type="ephemeral",
                )
        else:
            try:
                logger.debug(f"Body Text:{command_text}")
                db_name, schema_name = tuple(command_text.strip().split())
                channel = command["channel_id"]
                do_setup(
                    channel_id=channel,
                    client=client,
                    db_name=db_name,
                    schema_name=schema_name,
                    logger=logger,
                )
            except ValueError as e:
                respond(
                    text="Invalid format. Please provide both database name and schema name.",
                    response_type="ephemeral",
                )
            except Exception as e:
                logger.error(f"Setup error: {e}")
                respond(text=f"Error during setup: {str(e)}", response_type="ephemeral")
    except Exception as e:
        logger.error(f"Global handler error: {e}")
        try:
            respond(text="An unexpected error occurred.", response_type="ephemeral")
        except:
            # If respond fails, try using client as fallback
            client.chat_postEphemeral(
                channel=command["channel_id"],
                user=command["user_id"],
                text="An unexpected error occurred.",
            )


@app.action("setup_db")
def action_setup_db(ack, body, client, logger):
    setLogLevel(logger)
    logger.debug(f"Received Message Event: {body}")

    # Acknowledge the button click
    ack()

    # Get the input value
    db_name = body["state"]["values"]["db_name_input_block"]["db_name"]["value"]
    schema_name = body["state"]["values"]["schema_name_input_block"]["schema_name"][
        "value"
    ]
    channel = body["channel"]["id"]
    do_setup(
        channel_id=channel,
        client=client,
        db_name=db_name,
        schema_name=schema_name,
        logger=logger,
    )


@app.command("/cortalyst")
def handle_cortalyst(ack, client: WebClient, say, command, respond, logger):
    ack()
    logger.debug(f"Received Command 'cortalyst': {command}")
    try:
        command_text = command.get("text", "").strip()
        if not command_text:
            try:
                logger.debug(f"No question asking user")
                # Send the response with input block
                respond(
                    blocks=blocks.cortex_question,
                    response_type="ephemeral",  # Only visible to the user who triggered the command
                )
            except Exception as e:
                logger.error(f"Failed to send response: {e}")
                # Fallback response
                respond(
                    text="Sorry, there was an error displaying the question form.",
                    response_type="ephemeral",
                )
        else:
            logger.debug(f"Question:{command_text}")
            try:
                channel_id = command["channel_id"]
                ask_cortex_analyst(
                    channel_id=channel_id,
                    client=client,
                    say=say,
                    logger=logger,
                    question=command_text,
                )
            except Exception as e:
                logger.error(f"Cortalyst error: {e}")
                respond(
                    text=f"Error asking Cortex Analyst: {str(e)}",
                    response_type="ephemeral",
                )
    except Exception as e:
        logger.error(f"Cortalyst::Failed to send response: {e}")
        try:
            respond(text="An unexpected error occurred.", response_type="ephemeral")
        except:
            # If respond fails, try using client as fallback
            client.chat_postEphemeral(
                channel=command["channel_id"],
                user=command["user_id"],
                text="An unexpected error occurred.",
            )


@app.action("ask_cortex_analyst")
def action_ask_cortex_analyst(ack, body, client, respond, say, logger):
    ack()
    setLogLevel(logger)
    try:
        logger.debug(f"Received Message Event: {body}")
        global db_setup  # make sure we use the global one

        question = body["state"]["values"]["analyst_question_block"]["question"][
            "value"
        ]
        channel_id = body["channel"]["id"]
        ask_cortex_analyst(channel_id, client, say, logger, question)

    except Exception as e:
        logger.error(f"Failed to send request to Cortex Analyst: {e}")
        # Fallback response
        respond(
            text="Sorry, there was an error askingCortex Analyst .",
            response_type="ephemeral",
        )


def ask_cortex_analyst(channel_id: str, client: WebClient, say, logger, question: str):
    try:
        sanitized_question = " ".join(question.splitlines())

        logger.debug(f"Question:{sanitized_question}")
        logger.debug(f"Using DB:{db_setup.db_name},Schema:{db_setup.schema_name}")

        client.chat_postMessage(
            channel=channel_id,
            text=f":timer_clock: Wait for a few seconds... while I ask the Cortex Analyst :robot_face:",
        )

        if os.getenv("PRIVATE_KEY_FILE_PATH") is None:
            raise Exception(
                f"Require PRIVATE_KEY_FILE_PATH to be set. Consult Snowflake documentation https://docs.snowflake.com/user-guide/key-pair-auth#configuring-key-pair-authentication."
            )

        cortalyst = Cortlayst(
            account=session.conf.get("account"),
            user=session.conf.get("user"),
            host=session.conf.get("host"),
            private_key_file_path=os.getenv("PRIVATE_KEY_FILE_PATH"),
        )

        ans = cortalyst.answer(question)

        content = ans["message"]["content"]
        show_response(
            client,
            channel_id,
            content,
            say,
        )
    except Exception as e:
        raise Exception(e)


def show_response(client: WebClient, channel_id, content: List[Dict[str, Any]], say):
    try:
        for item in content:
            match item["type"]:
                case "sql":
                    # Send raw generated query for reference
                    logger.debug(f"Generating text block with generated SQL")
                    query = item["statement"]
                    say(
                        blocks=blocks.create_sql_block(query),
                        text="Generated SQL",
                    )

                    # Build and Display Dataframe for Query Results
                    logger.debug(f"Building query result")
                    df = session.sql(query).to_pandas()
                    say(
                        blocks=blocks.create_df_block(df),
                        text="Query Result",
                    )

                    # Visualization
                    # only I have enough columns for building a graph
                    if len(df.columns) > 1:
                        chart = (
                            alt.Chart(df)
                            .mark_arc()
                            .encode(theta="TICKET_COUNT", color="SERVICE_TYPE")
                        )

                        # Save chart to bytes buffer as PNG
                        buffer = io.BytesIO()
                        chart.save(buffer, format="png")
                        buffer.seek(0)
                        image_bytes = buffer.getvalue()

                        # Upload image bytes to Slack
                        uploaded_file = client.files_upload_v2(
                            channel=channel_id,
                            file=image_bytes,
                            filename="chart.png",
                            initial_comment="Generating chart...",
                        )

                        logger.info(f"Uploaded File:{uploaded_file}")

                        # say(
                        #     blocks=blocks.visualization_block(uploaded_file),
                        #     text="Query Result",
                        # )
                case _:
                    pass
    except Exception as e:
        logger.error(f"Error sending response {e}", exc_info=True)
        raise Exception(f"Error sending response {e}")


# Error handler
@app.error
def error_handler(error, body, logger):
    logger.error(f"Error: {error}")
    logger.error(f"Request body: {body}")


def main():
    logger.debug("Jai Guru! Starting Slack bot application...")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()


# Start your app
if __name__ == "__main__":
    main()
