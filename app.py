import os
import logging

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(levelname)s:%(message)s",
)
logger.setLevel(level=logging.DEBUG)


# Initializes your app with your bot token and socket mode handler
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))


@app.message("chant")
def chant_handler(message, say):
    logger.debug(f"Received Message {message}")
    say("Jai Guru!")


# Start your app
if __name__ == "__main__":
    logger.debug("Starting App::Jai Guru")
    SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"]).start()
