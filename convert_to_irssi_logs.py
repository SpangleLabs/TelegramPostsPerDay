import datetime
import json
import os
from collections import defaultdict

import telethon
import dateutil.parser
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageActionChatDeleteUser, MessageActionChatAddUser
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
    return (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)


def add_message_to_log(data, message, log_name):
    date = message.date.date().isoformat()
    time = message.date.time().isoformat()
    log_line = None
    if isinstance(message.action, MessageActionChatDeleteUser):
        log_line = f"{time} -!- {get_user_name(message.sender)} [~{message.sender.id}@Telegram] has quit [Left chat]"
    elif isinstance(message.action, MessageActionChatAddUser):
        log_line = f"{time} -!- {get_user_name(message.sender)} [~{message.sender.id}@Telegram] has joined {log_name}"
    elif message.text:
        log_line = f"{time} < {get_user_name(message.sender)}> {message.text}"
    elif message.media:
        log_line = f"{time} * {get_user_name(message.sender)} sent a picture {str(message.media)[:70]}..."
    if log_line is not None:
        data["log_by_date"][date].append(log_line)


async def parse_messages(client, chat_handle):
    chat_entity = await client.get_entity(chat_handle)
    log_name = f"#{chat_entity.title}" if hasattr(chat_entity, "title") else (get_user_name(chat_entity) or chat_entity.id)
    count = await get_message_count(client, chat_entity)
    data = {
        "log_by_date": defaultdict(lambda: [])
    }
    with tqdm(total=count) as bar:
        async for message in client.iter_messages(chat_entity):
            add_message_to_log(data, message, log_name)
            bar.update(1)
    for log_date, log in data["log_by_date"].items():
        pydate = dateutil.parser.parse(log_date)
        file_contents = [
            "--- Log opened " + pydate.strftime("%a %b %d 00:00:00 %Y"),
            *log[::-1]
        ]
        if pydate.date() != datetime.date.today():
            file_contents.append("--- Log closed " + (pydate + datetime.timedelta(1)).strftime("%a %b %d 00:00:00 %Y"))
        os.makedirs(f"irclogs/{pydate.year}", exist_ok=True)
        file_name = f"irclogs/{pydate.year}/{log_name}.{pydate.strftime('%m-%d')}.log"
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
