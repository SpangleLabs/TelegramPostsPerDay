import json
from collections import defaultdict

import telethon
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types.messages import Messages
from tqdm import tqdm

config = {
    "api_id": "",
    "api_hash": "",
    "chat_handle": ""
}


async def iter_channel_messages(client, channel_handle: str):
    channel_entity = await client.get_entity(channel_handle)
    async for message in client.iter_messages(channel_entity):
        yield message


async def get_message_count(client, channel_handle):
    chat_entity = await client.get_entity(channel_handle)
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


async def parse_messages(client, chat_handle):
    data = {
        "by_date": defaultdict(lambda: 0)
    }
    count = await get_message_count(client, chat_handle)
    data["count"] = count
    with tqdm(total=count) as bar:
        async for message in iter_channel_messages(client, chat_handle):
            date = message.date.date().isoformat()
            data["by_date"][date] += 1
            bar.update(1)
    return data


def run(conf):
    client = telethon.TelegramClient('post_counter', conf["api_id"], conf["api_hash"])
    client.start()
    data = client.loop.run_until_complete(parse_messages(client, conf["chat_handle"]))
    with open("output.json", "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    run(config)
