"""
openai-model.py

This code is part of a solution developed to the Innovation Challenge December 
2024 held by Microsoft. All the rights to this code are reserved to the development 
team (Adriano Godoy, Danillo Silva, Demerson Polli, Roberta Siqueira, and Rodica 
Varinu). This code is intended for private use of this team (the "Team") or by 
Microsoft.

The use of this software and the documentation files (the "Software") is granted to 
Microsoft as described in paragraph 7 of the "Microsoft Innovation Challenge Hackaton 
December 2024 Official Rules". The use of this software for any person or enterprise 
other than the Team or Microsoft requires explicit authorization by the Team, except 
for educational purposes given the corresponding credits for the authors (the Team). 
Any educational use without the corresponding credits will be considered a commercial 
use of the Software and is subject to the legal requirements of royalties.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR 
PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE 
FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY.
"""
import ast
import openai 
import os

import azure.cosmos.cosmos_client as cosmos_client

from dotenv import load_dotenv
from openai import AssistantEventHandler
from typing_extensions import override, List

from agrodata import AgroDatabase


def get_pesticides_by_crop_and_common_name(crop, common_name) -> List:
    """
    Retrieve a list of pesticide brand names for a given crop and common pest name.
    This function connects to a Cosmos DB instance, retrieves formulated products
    based on the common pest name, and returns a list of unique pesticide brand names.

    For demonstration purpose only, the crop will be ignored since the database only
    contains information about soy.

    Args:
        crop (str): The name of the crop for which pesticides are being queried.
        common_name (str|list): The common name of the pest or a list of names.
    
    Returns:
        list: A list of up to 5 unique pesticide brand names.
    """
    # Initialize the Cosmos client
    endpoint = os.getenv("COSMOS_DB_ENDPOINT")
    key = os.getenv("COSMOS_DB_KEY")
    client = cosmos_client.CosmosClient(endpoint, {'masterKey': key})

    # Create a database
    database_name = os.getenv("COSMOS_DB_DATABASE")
    database = client.create_database_if_not_exists(id= database_name)

    if type(common_name) == str:
        common_name = '-'.join(common_name.lower().split(' '))
        df = AgroDatabase.get_formulated_products_by_common_prague_name(database, common_name)
    elif type(common_name) == list:
        for k in range(len(common_name)):
            common_name[k] = '-'.join(common_name[k].lower().split(' '))
        df = AgroDatabase.get_formulated_products_by_common_prague_names(database, common_name)

    df.drop_duplicates(subset= ["MARCA_COMERCIAL"], inplace= True)

    return str(df['MARCA_COMERCIAL'].sample(5).tolist())


def get_pesticides_by_crop_and_scientific_name(crop, scientific_name) -> list:
    """
    Retrieve a list of pesticide brand names for a given crop and scientific pest name.
    This function connects to a Cosmos DB instance, retrieves formulated products
    based on the scientific pest name, and returns a list of unique pesticide brand names.

    For demonstration purpose only, the crop will be ignored since the database only
    contains information about soy.

    Args:
        crop (str): The name of the crop for which pesticides are being queried.
        scientific_name (str|list): The scientific name of the pest or a list of names.
    
    Returns:
        list: A list of up to 5 unique pesticide brand names.
    """
    # Initialize the Cosmos client
    endpoint = os.getenv("COSMOS_DB_ENDPOINT")
    key = os.getenv("COSMOS_DB_KEY")
    client = cosmos_client.CosmosClient(endpoint, {'masterKey': key})

    # Create a database
    database_name = os.getenv("COSMOS_DB_DATABASE")
    database = client.create_database_if_not_exists(id= database_name)

    if type(scientific_name) == str:
        df = AgroDatabase.get_formulated_products_by_cientific_prague_name(database, scientific_name)
    elif type(scientific_name) == list:
        df = AgroDatabase.get_formulated_products_by_cientific_prague_names(database, scientific_name)

    df.drop_duplicates(subset= ["MARCA_COMERCIAL"], inplace= True)

    return str(df['MARCA_COMERCIAL'].sample(5).tolist())


class EventHandler(AssistantEventHandler):
    @override
    def on_event(self, event):
        # Retrieve events that are denoted with 'requires_action'
        # since these will have our tool_calls.
        if event.event == "thread.run.requires_action":
            run_id = event.data.id   # Retrieve the run ID from the event data
            self.handle_requires_action(event.data, run_id)
    
    def handle_requires_action(self, data: openai.types.beta.threads.run.Run, run_id: str) -> None:
        tool_outputs= []

        for tool in data.required_action.submit_tool_outputs.tool_calls:
            if tool.function.name == "pesticide_by_crop_and_common_name":
                tool_outputs.append(
                    {
                        "tool_call_id": tool.id, 
                        "output": get_pesticides_by_crop_and_common_name(**ast.literal_eval(tool.function.arguments))
                    }
                )
            elif tool.function.name == "pesticide_by_crop_and_scientific_name":
                tool_outputs.append(
                    {
                        "tool_call_id": tool.id, 
                        "output": get_pesticides_by_crop_and_scientific_name(**ast.literal_eval(tool.function.arguments))
                    }
                )
        
        # Submit all tool_outputs at the same time
        self.submit_tool_outputs(tool_outputs, run_id)
    
    def submit_tool_outputs(self, tool_outputs: list, run_id: str) -> None:
        # Use the submit_tool_outputs_stream helper
        with client.beta.threads.runs.submit_tool_outputs_stream(
            thread_id= self.current_run.thread_id,
            #run_id= self.current_run.id,
            run_id= run_id,
            tool_outputs= tool_outputs,
            event_handler= EventHandler()
        ) as stream:
            for text in stream.text_deltas:
                print(text, end= "", flush= True)
            print()


if __name__ == "__main__":
    load_dotenv()

    openai.api_key = os.getenv("OPENAI_API_KEY")

    client = openai.OpenAI()
    
    # Step 1: Filtering questions by matter.
    classifier_assistant = client.beta.assistants.create(
        instructions= """
        You are a useful assistant especialized in filtering questions by subject.
        The user will ask you a question in Portuguese, and you will have to identify
        if the question is related to the topic of pesticides and crops. In this case
        you will answer "pesticides". For any question not related to the topic, you
        will answer "other". All interactions with the user will be in Portuguese.        
        """,
        model= "gpt-4o"
    )

    # Step 2: Create a thread and add messages
    questions = [
        "Qual os melhores pesticidas para tratar de flor de poeta em uma lavoura de soja?",
        "Qual os melhores pesticidas para tratar de Helicoverpa armigera em uma lavoura de café?",
        "Qual a capital da França?"       
    ]

    filtered_questions = []
    rejected_questions = []

    for question in questions:
        classifier_thread = client.beta.threads.create()
        classifier_message= client.beta.threads.messages.create(
            thread_id= classifier_thread.id,
            role= "user",
            content= question,
        )

        # Step 3: Initialize a run
        with client.beta.threads.runs.stream(
            thread_id= classifier_thread.id,
            assistant_id= classifier_assistant.id,
        ) as stream:
            stream.until_done()
        
        messages= client.beta.threads.messages.list(thread_id= classifier_thread.id)
        if messages.data[0].content[0].text.value == "pesticides":
            filtered_questions.append(question)
        else:
            rejected_questions.append(question)
        

    # Now we get the filtered questions and we can proceed with the next steps.
    # The idea is to create a new assistant for each filtered question and provide
    # the necessary tools for the assistant to work with the user.
    
    # Step 1: Define functions.
    assistant = client.beta.assistants.create(
        instructions= """
        You are a useful assistant especialized in prescribing pesticides for crops.
        The user will inform you about the crop and the pest, and you will provide 
        the best pesticide for the situation. For your responses, you may use the
        the provided functions. All interactions with the user will be in Portuguese.
        For any question not related to the task, you may gently answer "I don't know".
        """,
        model= "gpt-4o",
        tools= [
            {
                "type": "function",
                "function": {
                    "name": "pesticide_by_crop_and_common_name",
                    "description": "Get a list of efficient pesticides for a specific crop and common prague name.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "crop": {
                                "type": "string",
                                "description": "The name of a crop, e.g., 'soja', 'milho', etc."
                            },
                            "common_name": {
                                "type": "string",
                                "description": "The common name of a prague, e.g., 'flor de poeta'."
                            }
                        },
                        "required": ["crop", "common_name"],
                        "additionalProperties": False,
                    },
                    "strict": True
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "pesticide_by_crop_and_scientific_name",
                    "description": "Get a list of efficient pesticides for a specific crop and scientific prague name.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "crop": {
                                "type": "string",
                                "description": "The name of a crop, e.g., 'soja', 'café', etc."
                            },
                            "scientific_name": {
                                "type": "string",
                                "description": "The scientific name of a prague, e.g., 'Helicoverpa armigera'."
                            }
                        },
                        "required": ["crop", "scientific_name"],
                        "additionalProperties": False,
                    },
                    "strict": True
                }
            }
        ]
    )


    thread = client.beta.threads.create()
    for question in filtered_questions:
        # Step 2: Create a thread and add messages
        message= client.beta.threads.messages.create(
            thread_id= thread.id,
            role= "user",
            content= question,
        )

    # Step 3: Initialize a run
    with client.beta.threads.runs.stream(
        thread_id= thread.id,
        assistant_id= assistant.id,
        event_handler= EventHandler()
    ) as stream:
        stream.until_done()
