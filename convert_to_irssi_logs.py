import asyncio
import json
import sys
from typing import Dict

import telethon
from telethon.tl.types import InputPeerChannel

from telegram_logger.data_store import DataStore
from telegram_logger.database import Database
from telegram_logger.telegram_utils import get_chat_name


async def add_channel(data_store, client):
    print("- Listing new chat options:")
    dialogs = await client.get_dialogs()
    entities = [chan for chan in dialogs if isinstance(chan.input_entity, InputPeerChannel) and chan.entity.megagroup]
    chat_options = [{"id": entity.id, "name": get_chat_name(entity.entity)} for entity in entities]
    for option in range(len(chat_options)):
        print(f"- {option}: {chat_options[option]['name']}")
    selected_option = input("- Please enter the number for the channel you wish to add: ")
    if selected_option in ["", "n", "skip"]:
        print("- Aborting channel add")
        return False
    chat_option = chat_options[int(selected_option)]
    data_store.add_chat(chat_option["id"])
    print(f"- Added chat: {chat_option['name']}")
    more_channels = input("Would you like to add any more new chats? [n]: ")
    return more_channels.lower().strip() in ["yes", "y"]


async def ask_questions(data_store, client):
    chat_entities = await asyncio.gather(*(client.get_entity(chat_log.handle) for chat_log in data_store.chat_logs))
    chat_names = [get_chat_name(entity) for entity in chat_entities]
    print("- Currently generating stats for these channels: " + ", ".join(chat_names))
    more_channels = input("Would you like to add any new chats? [n]: ")
    if more_channels.lower().strip() not in ["yes", "y"]:
        return
    adding_channel = True
    while adding_channel:
        adding_channel = await add_channel(data_store, client)
    print(" - Finished adding new chats")


async def update_data(client, skip_questions: bool, db_conn_str: str):
    print("Setup database")
    database = Database(db_conn_str)
    print("Loading data store")
    data_store = DataStore.load_from_json(database)
    if not skip_questions:
        await ask_questions(data_store, client)
    print("Updating logs")
    await data_store.update_all_logs(client)
    print("Saving data store")
    data_store.save_to_json()
    print("Writing logs")
    await data_store.write_all_logs(client)
    print("Writing users config")
    await data_store.write_users_cfg(client)
    print("Writing channel config")
    await data_store.write_channel_cfg(client)


def run(conf: Dict, skip_questions: bool):
    client = telethon.TelegramClient('log_converter', conf["api_id"], conf["api_hash"])
    client.start()
    client.loop.run_until_complete(update_data(client, skip_questions, conf["db_conn"]))


if __name__ == "__main__":
    with open("config.json", "r") as conf_file:
        config = json.load(conf_file)
    run(config, "skip" in sys.argv)
