# %%
import asyncio
import json
from typing import Optional

import start

from udelar_graph.models import Person

# %%
with open("data/colibri_people.json", "r") as f:
    people = [Person.model_validate(p) for p in json.load(f)]
# %%
# Fix the linter error by adding a check
if people[10].aliases:
    people[10].aliases[0]

# %%
import openai
from pydantic import BaseModel

prompt = """\
You are an expert extracting people names from unstructured text. You'll be given a name in any format and will return a json object with the following format. If there is extra information on the text, like the institution, the position, etc, you should return it in the "institution" and "department" fields. The names are mostly in spanish and can appear in any order.
{
    "surnames": "string",
    "first_names": "string",
    "institution": "string" | None,
    "department": "string" | None,
    "person": bool # True if the text is a person name, False otherwise
}
"""


class Name(BaseModel):
    surnames: str
    first_names: str
    institution: str | None = None
    department: str | None = None
    person: bool


client = openai.OpenAI()

# %%
response = client.responses.parse(
    model="gpt-4o-mini",
    temperature=0.0,
    input=[
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": "Friss de Kereki Juan Andrés, Univeresidad de la República (Uruguay). Facultad de Ingeniería. Instituto de Ingeniería Eléctrica",
        },
    ],
    text_format=Name,
)
response.output_parsed
# %%
response.usage
# %%
import asyncio
from asyncio import Semaphore

from tqdm.asyncio import tqdm as tqdm_async

MODEL = "gpt-4o-mini"


async def process_person_async(
    person: Person,
    client: openai.AsyncOpenAI,
    semaphore: Semaphore,
    prompt: str,
    pbar: tqdm_async,
) -> tuple[str, Optional[Name]]:
    """Process a single person asynchronously with semaphore control."""
    async with semaphore:
        longest_alias = max(person.aliases, key=len) if person.aliases else None
        if not longest_alias:
            return person.normalized_name, None

        try:
            response = await client.responses.parse(
                temperature=0.0,
                model=MODEL,
                input=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": longest_alias},
                ],
                text_format=Name,
            )
            pbar.update(1)
            return person.normalized_name, response.output_parsed
        except Exception as e:
            print(f"Error processing {person.normalized_name}: {e}")
            return person.normalized_name, None


async def process_people_async(
    client: openai.AsyncOpenAI, people_to_process: list[Person]
) -> dict[str, Name]:
    """Process all people asynchronously with semaphore control."""
    # Create semaphore to limit concurrent requests to 10
    semaphore = Semaphore(10)

    pbar = tqdm_async(total=len(people_to_process), desc="Processing people")
    tasks = [
        process_person_async(person, client, semaphore, prompt, pbar)
        for person in people_to_process
    ]

    results = await asyncio.gather(*tasks)

    # Filter out None results and create the dictionary
    extracted_names: dict[str, Name] = {}
    for name, extracted_name in tqdm_async(results):
        if extracted_name is not None:
            extracted_names[name] = extracted_name

    return extracted_names


# Run the async processing
async_client = openai.AsyncOpenAI()
extracted_names = await process_people_async(async_client, people)


# %%
for i, (name, extracted_name) in enumerate(extracted_names.items()):
    person = people[i]
    longest_alias = max(person.aliases, key=len) if person.aliases else None
    print(longest_alias)
    print(extracted_name)
    print("-" * 100, "\n")
# %%
extracted_names_json = {
    k: v.model_dump(mode="json") for k, v in extracted_names.items()
}
with open("data/extracted_names.json", "w") as f:
    json.dump(extracted_names_json, f, indent=4)
# %%
