import json
import os
from collections import defaultdict

import telethon
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageActionChatDeleteUser, MessageActionChatAddUser
from telethon.tl.types.messages import Messages
from tqdm import tqdm

config = {
    "api_id": "",
    "api_hash": "",
    "chat_handle": ""
}



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


def new_day_data():
    return {
        "total": 0,
        "by_user": defaultdict(lambda: 0)
    }


def get_user_name(user):
    return (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)


def add_message_to_data(data, message):
    date = message.date.date().isoformat()
    data["posts_by_date"][date]["total"] += 1
    data["posts_by_date"][date]["by_user"][message.sender.id] += 1
    data["total_by_user"][message.sender.id] += 1
    if message.sender.id not in data["users"]:
        data["users"][message.sender.id] = {
            "name": get_user_name(message.sender),
            "username": message.sender.username,
            "id": message.sender.id
        }
    if not message.text and not message.media:
        if isinstance(message.action, MessageActionChatDeleteUser):
            data["membership_changes"][date].append({
                "action": "User left",
                "user_id": message.sender.id
            })
        elif isinstance(message.action, MessageActionChatAddUser):
            data["membership_changes"][date].append({
                "action": "User joined",
                "user_id": message.sender.id
            })


async def parse_messages(client, chat_handle):
    chat_entity = await client.get_entity(chat_handle)
    chat_name = chat_entity.title if hasattr(chat_entity, "title") else get_user_name(chat_entity)
    count = await get_message_count(client, chat_entity)
    data = {
        "chat": {
            "id": chat_entity.id,
            "title": chat_name
        },
        "users": {},
        "membership_changes": defaultdict(lambda: []),
        "posts_by_date": defaultdict(new_day_data),
        "total_count": count,
        "total_by_user": defaultdict(lambda: 0)
    }
    with tqdm(total=count) as bar:
        async for message in client.iter_messages(chat_entity):
            add_message_to_data(data, message)
            bar.update(1)
    return data


def run(conf):
    client = telethon.TelegramClient('post_counter', conf["api_id"], conf["api_hash"])
    client.start()
    data = client.loop.run_until_complete(parse_messages(client, conf["chat_handle"]))
    os.makedirs("stats/", exist_ok=True)
    with open(f"stats/output-{conf['chat_handle']}.json", "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    run(config)
