import os
import logging

from snowflake.snowpark.session import Session
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from handler_tasks.db_setup import DBSetup
from handler_tasks.blocks import db_schema_setup

_log_level = os.getenv("APP_LOG_LEVEL", "WARNING")

logging.basicConfig(
    level=logging.WARNING,
    format="%(name)s:%(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger("demo_mate_bot")
logger.setLevel(level=_log_level)

session = Session.builder.getOrCreate()

# Get the JWT Token to call the Cortex REST API
# try:
#     b_jwt = subprocess.check_output(["snow", "connection", "generate-jwt"])
#     jwt = b_jwt.decode().strip("\n")
#     logger.debug(f"JWT {jwt}")
# except Exception as e:
#     logger.error(e, stack_info=True)
#     sys.exit(1)

# Initializes your app with your bot token and socket mode handler
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))


def setLogLevel(logger):
    """
    Set the logger level to APP_LOG_LEVEL env
    """
    logger.setLevel(_log_level)


def do_setup(
    client,
    channel_id,
    logger,
    db_name: str = "demo_db",
    schema_name: str = "data",
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

        db_setup = DBSetup(session)
        db_setup.do(db_name=db_name, schema_name=schema_name)

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


@app.event("message")
def message_event_handler(ack, body, say, logger):
    ack()
    logger.debug(f"Received Message Event: {body}")
    say("good")


@app.command("/setup")
def setup_handler(ack, _body, client, command, respond):
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
                    blocks=db_schema_setup,
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
def cortalyst_handler(ack, say, command, logger):
    ack()
    logger.debug(f"Received Command 'cortalyst': {command}")
    say("ask_cortex_analyst")


def ask_cortex_analyst(ack, action, say, logger):
    ack()
    logger.debug(f"Action Command 'cortalyst': {action}")
    say("done asking")


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
