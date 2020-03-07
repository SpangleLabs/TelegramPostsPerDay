import json
from collections import defaultdict

import telethon

config = {
    "api_id": "",
    "api_hash": "",
    "chat_handle": ""
}


async def iter_channel_messages(client, channel_handle: str):
    channel_entity = await client.get_entity(channel_handle)
    async for message in client.iter_messages(channel_entity):
        yield message


async def parse_messages(client, chat_handle):
    data = {
        "by_date": defaultdict(lambda: 0)
    }
    async for message in iter_channel_messages(client, chat_handle):
        date = message.date.date().isoformat()
        data["by_date"][date] += 1
    return data


def run(conf):
    client = telethon.TelegramClient('post_counter', conf["api_id"], conf["api_hash"])
    client.start()
    data = client.loop.run_until_complete(parse_messages(client, conf["chat_handle"]))
    with open("output.json", "w") as f:
        json.dump(data, f, indent=2)


if __name__ == "__main__":
    run(config)
