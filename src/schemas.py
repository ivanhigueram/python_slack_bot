import json
from typing import List, Optional, TypedDict

import pandas as pd
from langchain_core.pydantic_v1 import BaseModel, Field


class Candidate(BaseModel):
    """Schema about a candidate to feed LLM."""

    name: Optional[str] = Field(..., description="The name of the candidate")
    undergraduate_institution: Optional[str] = Field(
        ..., description="Undergraduate institution"
    )
    graduate_institution: Optional[str] = Field(..., description="Graduate institution")
    program_major: Optional[str] = Field(
        ..., description="Program or current major of candidate"
    )
    advisor: Optional[str] = Field(
        ..., description="Advisor or supervisor at school or workplace"
    )
    current_workplace: Optional[str] = Field(
        ..., description="Current role and workplace"
    )
    current_project_name: Optional[str] = Field(
        ..., description="Summarize the current research project to a single name"
    )
    email: Optional[str] = Field(..., description="The email of the candidate")
    quality_assessment: Optional[str] = Field(
        ...,
        description="Quality assessment of the candidate based on the email and writting quality of the text from 0 to 10. 10 being the best.",
    )
    overall_summary: Optional[str] = Field(
        ..., description="Overall summary of the email text"
    )


class Data(BaseModel):
    """Extracted data about candidates."""

    people: List[Candidate]

    def data_to_pandas(self):
        """Convert data to pandas DataFrame."""

        dict_data = json.loads(self.json())["people"][0]

        return pd.DataFrame(dict_data, index=[0])


class Example(TypedDict):
    """A representation of an example consisting of text input and expected tool calls.

    For extraction, the tool calls are represented as instances of pydantic model.
    """

    input: str  # This is the example text
    tool_calls: List[BaseModel]  # Instances of pydantic model that should be extracted
