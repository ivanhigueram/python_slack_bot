import sqlite3
import pandas as pd


def create_database(data_path="data/slackbot_messages.db"):
    # Create SQLite connection
    conn = sqlite3.connect(data_path)
    c = conn.cursor()

    # Create messages table if it doesn't exist
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS messages
        (text TEXT, files TEXT, upload TEXT, user TEXT, display_as_bot TEXT,
        type TEXT, ts INT, client_msg_id TEXT, team TEXT, 
        reply_count INT, reply_users_count INT, is_locked TEXT,
        subscribed TEXT, channel_id TEXT, file_1 TEXT, file_2 TEXT, file_3 TEXT, 
        file_4 TEXT, file_5 TEXT, PRIMARY KEY (ts, client_msg_id) ON CONFLICT IGNORE)
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS parsed_messages
        (name TEXT, undergraduate_institution TEXT, graduate_institution TEXT, program_major TEXT,
        advisor TEXT, current_workplace TEXT, current_project_name TEXT, email TEXT, quality_assessment TEXT, overall_summary TEXT, channel_id TEXT, ts INT, PRIMARY KEY (ts) ON CONFLICT IGNORE)
        """
    )

    channels = pd.DataFrame(
        {
            "channel_id": ["C06PSDC08AX", "C06Q5A168DP", "C06PRB2EX61"],
            "channel_name": [
                "graduate_students",
                "research_assistants",
                "visiting_scholars",
            ],
        }
    ).to_sql("channels", conn, if_exists="replace", index=False)

    users = pd.DataFrame(
        {
            "user_id": ["U06N7CSQQKZ", "WBA9HFDCL", "U023Q2A64BU"],
            "user_name": ["Chumi", "Sam", "Me"],
        }
    ).to_sql("users", conn, if_exists="replace", index=False)

    del channels, users

    return conn, c


def structure_messages(messages, additional_cols=None):
    """Parse messages JSON from Slack API

    Args:
        messages (list): A list of messages from the Slack API
        additional_cols (dict): A dictionary of additional data to add to the dataframe. This can be a dictionary of keys with the column names and the values as the data to add.

    Returns:
        pd.DataFrame: A DataFrame of messages
    """
    cols_with_non_serials = [
        "files",
        "edited",
        "blocks",
        "thread_ts",
        "reply_users",
        "latest_reply",
        "subtype",
        "name",
        "attachments",
        "old_name",
    ]

    df = pd.DataFrame(messages)
    df = df.drop(columns=cols_with_non_serials, errors="ignore")

    # Convert timestamp to integer for ease
    df["ts"] = df["ts"].astype(float).astype(int)

    # Add additional columns if provided
    if additional_cols:
        for col, data in additional_cols.items():
            df[col] = data

    return df
