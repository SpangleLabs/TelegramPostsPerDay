import datetime
import json
import os
from collections import defaultdict

import telethon
import dateutil.parser
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageActionChatDeleteUser, MessageActionChatAddUser, MessageMediaDocument, \
    MessageMediaPhoto
from telethon.tl.types.messages import Messages
from tqdm import tqdm


async def get_message_count(client, chat_entity):
    get_history = GetHistoryRequest(
        peer=chat_entity,
        offset_id=0,
        offset_date=None,
        add_offset=0,
        limit=1,
        max_id=0,
        min_id=0,
        hash=0
    )
    history = await client(get_history)
    if isinstance(history, Messages):
        count = len(history.messages)
    else:
        count = history.count
    return count


def get_user_name(user):
    full_name = (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)
    if full_name == "":
        return "DELETED_ACCOUNT"
    return full_name.replace(" ", "_")


def add_message_to_log(data, message, log_name):
    date = message.date.date().isoformat()
    time = message.date.time().isoformat()
    log_lines = []
    if isinstance(message.action, MessageActionChatDeleteUser):
        log_lines = [f"{time} -!- {get_user_name(message.sender)} [~{message.sender.id}@Telegram] has quit [Left chat]"]
    elif isinstance(message.action, MessageActionChatAddUser):
        log_lines = [f"{time} -!- {get_user_name(message.sender)} [~{message.sender.id}@Telegram] has joined {log_name}"]
    elif message.text:
        log_lines = [f"{time} < {get_user_name(message.sender)}> {text}" for text in message.text.split("\n")[::-1]]
    elif message.media and isinstance(message.media, MessageMediaDocument):
        log_lines = [f"{time} * {get_user_name(message.sender)} sent a document ID={message.media.document.id}"]
    elif message.media and isinstance(message.media, MessageMediaPhoto):
        log_lines = [f"{time} * {get_user_name(message.sender)} sent a photo ID={message.media.photo.id}"]
    data["log_by_date"][date] += log_lines


def get_file_name(log_name, log_date):
    return f"irclogs/{log_date.year}/{log_name}.{log_date.strftime('%m-%d')}.log"


def last_date_for_log(log_name):
    today = datetime.date.today()
    limit = 100
    for x in range(limit):
        log_date = today - datetime.timedelta(days=x)
        file_name = get_file_name(log_name, log_date)
        try:
            with open(file_name, "r") as f:
                last_line = f.readlines()[-1]
                if last_line.startswith("--- Log closed"):
                    return log_date
        except FileNotFoundError:
            continue
    return None


async def parse_messages(client, chat_handle):
    chat_entity = await client.get_entity(chat_handle)
    log_name = f"#{chat_entity.title}" if hasattr(chat_entity, "title") else (get_user_name(chat_entity) or chat_entity.id)
    count = await get_message_count(client, chat_entity)
    data = {
        "log_by_date": defaultdict(lambda: [])
    }
    last_date = last_date_for_log(log_name)
    with tqdm(total=count) as bar:
        async for message in client.iter_messages(chat_entity):
            if last_date is None or message.date.date() > last_date:
                add_message_to_log(data, message, log_name)
            bar.update(1)
    for log_date_str, log in data["log_by_date"].items():
        log_date = dateutil.parser.parse(log_date_str)
        file_contents = [
            "--- Log opened " + log_date.strftime("%a %b %d 00:00:00 %Y"),
            *log[::-1]
        ]
        if log_date.date() != datetime.date.today():
            file_contents.append("--- Log closed " + (log_date + datetime.timedelta(1)).strftime("%a %b %d 00:00:00 %Y"))
        os.makedirs(f"irclogs/{log_date.year}", exist_ok=True)
        file_name = get_file_name(log_name, log_date)
        with open(file_name, "w", encoding="utf-8") as f:
            f.write("\n".join(file_contents))


def run(conf):
    client = telethon.TelegramClient('log_converter', conf["api_id"], conf["api_hash"])
    client.start()
    client.loop.run_until_complete(parse_messages(client, conf["chat_handle"]))


if __name__ == "__main__":
    with open("config.json", "r") as conf_file:
        config = json.load(conf_file)
    handle = input("Enter chat handle: ")
    try:
        config["chat_handle"] = int(handle)
    except ValueError:
        config["chat_handle"] = handle
    run(config)
