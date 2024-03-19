# HR Slack Bot

This is a Slack app using the Python `slack-sdk` to extract emails from a certain list of channels
and parse them to a structured schema and push the data to a Google Spreadsheet. The bot tries
to have the same functionality as other bots using *slash* commands. 

## What's happening under the hood? 

As a email body gets posted from one of the observed Slack channels, the bot uses the Slack SDK
to retrieve the messages as a JSON string and push them to a SQLite database stored locally (ofc)
We use Slack's rounded timestamp ( `ts`) as the primary key for each message, so we are only adding
the messages that are new to the `messsages` table. 

Emails are parsed using `gpt-3-1102` with no temperature to avoid generation and the data is
retrieved using a data schema (see `src/schemas.py`). The data is taken from the table `messages`
in the SQL database (`text` column, following the `slack-sdk` standard). Once processed, the parsed
data is stored in the `parsed_messages` table with the `ts` identifier and the `channel_id`, and
later pushed to a Google Spreadsheet. 

$$ Language model configuration
 - Prompting design happens in `src/extractor.py`, but changes to the prompt are done in other 
 parts of the script. We follow the following prompting strategy:
 ```
 main_prompt -> examples -> response_schema 
 ```
 - Examples are a way to retrieve more accurate responses for data that the model hasn't seen. We
 use the examples as a way to improve retrieval and we can define better examples in `data/examples.json`
 Notice any example should include a text prompt and the outcome we want. 


