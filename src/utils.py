import json

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from langchain_community.llms import Ollama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from tqdm import tqdm

from .schemas import Candidate


def send_messages_to_google_spreadsheet(parsed_messages, credentials, conn):
    """Send parsed messages to Google Spreadsheet

    This function will take a list of parsed messages and send them to a Google Spreadsheet.

    Args:
        parsed_messages (list): A list of parsed messages
        credentials (str): The path to the JSON file with the Google Service Account credentials

    Returns:
        None
    """

    # Build a data frame with the records in list

    if parsed_messages is not None:
        data = pd.concat(parsed_messages)

        # Format a nice table using SQL to upload to Google Sheets
        data.to_sql(name="parsed_messages", con=conn, if_exists="append", index=False)

    # Send clean data to Google Spreadsheet by replacing channel_id with the channel name
    data_tidy = pd.read_sql_query(
        """
        SELECT pm.name, pm.undergraduate_institution, pm.graduate_institution, pm.program_major, pm.advisor, pm.current_workplace, pm.current_project_name, pm.email, pm.quality_assessment, pm.overall_summary, c.channel_name, pm.ts, m.file_1, m.file_2, m.file_3, m.file_4, m.file_5
        FROM parsed_messages pm
        LEFT JOIN channels c ON pm.channel_id = c.channel_id
        LEFT JOIN messages m ON pm.ts = m.ts
        """,
        conn,
    )

    # Transform the ts column to a datetime object and add the current processing date
    data_tidy["ts"] = pd.to_datetime(data_tidy["ts"], unit="s")
    data_tidy["processing_date"] = pd.to_datetime("today")

    # Send data to Google Spreadsheet
    if credentials:
        gc = gspread.service_account(filename=credentials)
    else:
        gc = gspread.service_account()

    spreadsheet = gc.open("test_candidates")
    worksheet = spreadsheet.get_worksheet(0)

    set_with_dataframe(worksheet, data_tidy)

    return None


def load_examples(path_to_examples):
    """Parse examples in JSON as a list of Candidate objects"""

    # Load examples
    with open(path_to_examples, "r") as f:
        examples = json.load(f)

    # Convert examples to Candidate objects
    candidates = []
    for example in examples:
        text = example["text"]
        example.pop("text")
        candidate_example = tuple([text, Candidate(**example)])

        candidates.append(candidate_example)

    return candidates


def parse_messages(
    client_db, channel_id, table_name="parsed_messages", files_path=None
):
    """Parse messages using LangChain with Llama

    This function will take a JSON file with messages and files liked to the messages to parse the text in the JSON file and build a chain of extraction using LangChain with Llama using a prompting chain. The function will then return a list of parsed messages.

    Args:
        json_path (str): The path to the JSON file with the messages
        files_path (str): The path to the folder with the files

    Returns:
        list: A list of parsed messages
    """

    # Get messages from SQLite database and save them as a list
    messages = pd.read_sql_query(
        f"""SELECT text
        FROM messages
        WHERE channel_id = {channel_id}
        """,
        client_db,
    )["text"].tolist()

    llm = Ollama(model="llama2")

    # Start prompting chain
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """
            You are an HR manager at a research lab. You want to identify possible candidates 
            for a new position. You have access to emails from different candidates and you want
            to summarize the data from these emails to make a decision. Return a summary of the email information: name of the candidate, candidate's email, undergraduate institution, graduate institution, advisor, current role and company, and an overall summary of the email text. Please format all the information using colons (:) and separate each field with a new line. For example: name: John Doe\nemail: jdoe@company.com\nundergraduate institution: University of California, Berkeley\ngraduate institution: Stanford University\nadvisor: Dr. Jane Smith\ncurrent role and company: Data Scientist at Google\noverall summary: ...
            """,
            ),
            ("user", "{input}"),
        ]
    )

    # Build chain
    output_parser = StrOutputParser()
    chain = prompt | llm | output_parser

    parsed_messages = []
    for message in tqdm(messages, desc="Parsing messages using LLM"):
        summary = chain.invoke({"input": message})
        parsed_messages.append(summary)

    return parsed_messages
