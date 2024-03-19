import uuid
from typing import List

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    ToolMessage,
)
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI

from .schemas import Example, Data
from .utils import load_examples


def tool_example_to_messages(example: Example) -> List[BaseMessage]:
    """Convert an example into a list of messages that can be fed into an LLM.

    This code is an adapter that converts our example to a list of messages
    that can be fed into a chat model.

    The list of messages per example corresponds to:

    1) HumanMessage: contains the content from which content should be extracted.
    2) AIMessage: contains the extracted information from the model
    3) ToolMessage: contains confirmation to the model that the model requested a tool correctly.

    The ToolMessage is required because some of the chat models are hyper-optimized for agents
    rather than for an extraction use case.
    """
    messages: List[BaseMessage] = [HumanMessage(content=example["input"])]
    openai_tool_calls = []
    for tool_call in example["tool_calls"]:
        openai_tool_calls.append(
            {
                "id": str(uuid.uuid4()),
                "type": "function",
                "function": {
                    "name": tool_call.__class__.__name__,
                    "arguments": tool_call.json(),
                },
            }
        )
    messages.append(
        AIMessage(content="", additional_kwargs={"tool_calls": openai_tool_calls})
    )

    tool_outputs = example.get("tool_outputs") or [
        "You have correctly called this tool."
    ] * len(openai_tool_calls)

    for output, tool_call in zip(tool_outputs, openai_tool_calls):
        messages.append(ToolMessage(content=output, tool_call_id=tool_call["id"]))

    return messages


def run_model_w_examples(text_prompt, example_data):
    """Run the model with examples and text."""

    # Create a chat prompt
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You are an expert email extraction algorithm and an HR manager "
                "You watn to pick the best candidates for a new position."
                "Only extract relevant information from the text. "
                "If you do not know the value of an attribute asked "
                "to extract, return null for the attribute's value.",
            ),
            MessagesPlaceholder("examples"),
            ("human", "{text}"),
        ]
    )

    # Load examples for in-context inference
    if isinstance(example_data, str):
        examples = load_examples(example_data)
    else:
        examples = example_data

    # Add the examples to the prompt
    messages = []
    for text, tool_call in examples:
        messages.extend(
            tool_example_to_messages({"input": text, "tool_calls": [tool_call]})
        )

    # Create LLM object
    llm = ChatOpenAI(
        model="gpt-3.5-turbo-0125",
        temperature=0,  # because probably is better for extraction
    )

    runnable = prompt | llm.with_structured_output(
        schema=Data,
        method="function_calling",
        include_raw=False,
    )

    return runnable.invoke(
        {
            "text": text_prompt,
            "examples": messages,
        }
    )
