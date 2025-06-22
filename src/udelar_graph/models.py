from pydantic import BaseModel, Field


class Person(BaseModel):
    normalized_name: str
    aliases: list[str] = Field(default_factory=list)
    names: str | None = None
    surnames: str | None = None


class Work(BaseModel):
    normalized_title: str
    title: str | None = None
    abstract: str | None = None
    type: str | None = None
    pdf_url: str | None = None
    source: str | None = None
    language: str | None = None


class WorkType(BaseModel):
    type: str


class WorkKeyword(BaseModel):
    keyword: str
