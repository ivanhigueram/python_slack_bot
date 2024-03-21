import logging
import os
import sqlite3

import pandas as pd
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from tqdm import tqdm

from src.retrieve_messages import parsing_messages, retrieve_messages
from src.utils import send_messages_to_google_spreadsheet

logging.basicConfig(level=logging.DEBUG)

# Set up the Slack client
slack_token = os.getenv("SLACK_API_TOKEN")
slack_bot_token = os.getenv("SLACK_BOT_TOKEN")
slack_secret = os.getenv("SLACK_SIGNING_SECRET")
client = WebClient(token=slack_token)
bolt_app = App(token=slack_token)


@bolt_app.event("app_mention")
def event_test(say):
    say(
        {
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "I am research procastination at its finest, but also a HR assistant. After a posting, please reload me we can add the candidates to the database.",
                    },
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "Access the candidate database"},
                    "accessory": {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "🆒", "emoji": True},
                        "value": "click_me_123",
                        "url": "https://docs.google.com/spreadsheets/d/1rfNE7-SP3sYEWDg3tWw27lS1L8njRJ4QlXj1SqgQCf0/edit?usp=sharing",
                        "action_id": "button-action",
                    },
                },
            ]
        }
    )


@bolt_app.command("/summary")
def summary_command(say, ack):
    ack("Querying database... 👨🏽‍💻")

    conn = sqlite3.connect("/home/topcat/projects/python_slack_bot/data/slackbot_messages.db")

    query = """
        WITH table_group AS (
        SELECT pm.name, pm.undergraduate_institution, pm.graduate_institution, pm.program_major, pm.advisor, pm.current_workplace, pm.current_project_name, pm.email, pm.quality_assessment, pm.overall_summary, c.channel_name, pm.ts, m.file_1, m.file_2, m.file_3, m.file_4, m.file_5
        FROM parsed_messages pm
        LEFT JOIN channels c ON pm.channel_id = c.channel_id
        LEFT JOIN messages m ON pm.ts = m.ts
        ) select channel_name, count(name) as count from table_group group by channel_name order by 2;
        """

    df = pd.read_sql_query(query, conn)

    # Send message to Slacks
    say(
        {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "Number of candidates by type:\n\n",
                                }
                            ],
                        },
                        {
                            "type": "rich_text_preformatted",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": f"{df.to_markdown(index=False)}",
                                }
                            ],
                        },
                    ],
                }
            ]
        }
    )


@bolt_app.command("/reload")
def reload_command(say, ack):
    channel_ids = ["C06PSDC08AX", "C06Q5A168DP", "C06PRB2EX61"]
    poster_ids = ["U06N7CSQQKZ", "WBA9HFDCL"]
    save_data = "/home/topcat/projects/python_slack_bot/data/downloads"

    conn = sqlite3.connect("/home/topcat/projects/python_slack_bot/data/slackbot_messages.db")
    """Reload database to include new candidates in channel"""
    ack("Loading database... 👨🏽‍💻")
    # Retrieve messages from the channel
    for channel_id in tqdm(channel_ids, desc="Retrieving messages from channels"):
        retrieve_messages(
            client,
            channel_id,
            filter_users=poster_ids,
            save_data=save_data,
            db_conn=conn,
            messages_table="messages",
        )

    # Parse messages
    parsed_messages = parsing_messages(conn)

    # Send parsed messages to Google Spreadsheet
    send_messages_to_google_spreadsheet(
        parsed_messages, credentials="creds.json", conn=conn
    )

    # Send message to Slacks
    say(text="Database reloaded successfully! 🚀")


if __name__ == "__main__":
    SocketModeHandler(
        bolt_app, app_token=slack_bot_token, web_client=client, trace_enabled=True
    ).start()
