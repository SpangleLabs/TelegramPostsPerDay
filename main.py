from typing import Callable

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


def run(conf):
    data = {}
    data["by_date"] = {}
    data["membership_changes"] = []
    client = telethon.TelegramClient('post_counter', conf["api_id"], conf["api_hash"])
    async for message in iter_channel_messages(client, conf["chat_handle"]):
        print(message)


if __name__ == "__main__":
    run(config)