db_schema_setup = (
    [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Please provide me with Databse Name and Schema name to use with your demo setup:",
            },
        },
        {
            "type": "input",
            "block_id": "db_name_input_block",
            "element": {
                "type": "plain_text_input",
                "action_id": "db_name",
                "placeholder": {"type": "plain_text", "text": "SLACK_DEMO"},
            },
            "label": {"type": "plain_text", "text": "Database"},
        },
        {
            "type": "input",
            "block_id": "schema_name_input_block",
            "element": {
                "type": "plain_text_input",
                "action_id": "schema_name",
                "placeholder": {"type": "plain_text", "text": "DATA"},
            },
            "label": {"type": "plain_text", "text": "Schema"},
        },
        {
            "type": "actions",
            "block_id": "actions_block",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Submit"},
                    "action_id": "setup_db",
                }
            ],
        },
    ],
)
