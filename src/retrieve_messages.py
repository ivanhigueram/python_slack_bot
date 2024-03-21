import os

import pandas as pd
import requests
from slack_sdk.errors import SlackApiError
from tqdm import tqdm

from .extractor import run_model_w_examples
from .database_manager import structure_messages


def retrieve_messages(
    client,
    channel_id,
    db_conn,
    messages_table,
    save_data="data",
    filter_users=None,
    download=False,
):
    """Retrieve messages from a Slack channel and filter them by user

    Args:
        client (slack_sdk.WebClient): The Slack client
        channel_id (str): The ID of the channel to retrieve messages from
        db_path (str): Path to the SQLite database
        messages_table (str): Name of the table to save the messages
        filter_users (list): A list of user IDs to filter messages by
        save_data (str): The path to the folder to save the files

    Returns:
        list: A list of messages from the channel
    """

    # Create folder if save_data doesn't exist
    if not os.path.exists("data/downloads"):
        os.makedirs("data/downloads")

    # Check if filename exists (run only daily, best case scenario)
    try:
        # Retrieve messages from the channel
        result = client.conversations_history(channel=channel_id)

        # Filter messages by user
        if filter_users:
            messages = [
                message
                for message in result.data["messages"]
                if message["user"] in filter_users and "inviter" not in message.keys()
            ]
        else:
            messages = result.data["messages"]

        # Remove messages that are channel events
        messages = [message for message in messages if "subtype" not in message.keys()]

        # Download files if any of the messages has a file
        for message in messages:
            # We have files as dict format in a list

            if "files" in message.keys():
                for idx, file in enumerate(message["files"]):
                    file_url = file["url_private"]

                    message[f"file_{idx + 1}"] = file_url

                    if download:
                        if file["filetype"] == "pdf":
                            file_path = os.path.join(save_data, file["id"])

                            # Use the token for authentication
                            token = os.getenv("SLACK_API_TOKEN")
                            headers = {"Authorization": f"Bearer {token}"}

                            response = requests.get(file_url, headers=headers)
                            with open(f"{file_path}.pdf", "wb") as f:
                                f.write(response.content)

    except SlackApiError as e:
        print(f"Error: {e.response['error']}")

    # Parse messages

    # Create dict with additional columns
    additional_columns = {"channel_id": channel_id}

    df = structure_messages(
        messages,
        additional_cols=additional_columns,
    )

    # Update database
    df.to_sql(messages_table, db_conn, if_exists="append", index=False)

    return messages


def parsing_messages(conn):
    """Parse messages using LLM"""

    # Get messages from SQLite database and save them as a list
    messages = pd.read_sql_query(
        """SELECT text, channel_id, ts
            FROM messages
            WHERE ts NOT IN (SELECT ts FROM parsed_messages)
            """,
        conn,
    )

    if messages.empty:
        print("No new messages to parse")
        return

    # Create a list of parsed messages
    parsed_messages = []
    for index, row in tqdm(
        messages.iterrows(), total=messages.shape[0], desc="Parsing messages"
    ):
        try:
            data = run_model_w_examples(row["text"], "data/examples.json")
            df = data.data_to_pandas()

            # Add columns to add context
            df["channel_id"] = row["channel_id"]
            df["ts"] = row["ts"]

            parsed_messages.append(df)
        except Exception as e:
            print(f"Error: {e}")
            pass

    return parsed_messages
