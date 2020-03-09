import datetime
import json
import os
from collections import defaultdict
from typing import Optional, List, Set, Dict

import telethon
import dateutil.parser
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageActionChatDeleteUser, MessageActionChatAddUser, MessageMediaDocument, \
    MessageMediaPhoto
from telethon.tl.types.messages import Messages
from tqdm import tqdm


class DataStore:

    def __init__(self, chat_handles: Optional[List] = None, user_ids: Optional[Set] = None):
        self.chat_handles = chat_handles or []
        self.user_ids = user_ids or set()
        self.chat_logs = [ChatLog.load_from_json(chat_handle) for chat_handle in tqdm(self.chat_handles)]
        self.user_extra_data = {}

    def save_to_json(self):
        os.makedirs("irclogs_cache", exist_ok=True)
        with open("irclogs_cache/data_store.json", "w", encoding="utf-8") as f:
            json.dump({
                "chat_handles": self.chat_handles,
                "user_ids": list(self.user_ids),
                "user_extra_data": self.user_extra_data
            }, f, indent=2)
        for chat_log in self.chat_logs:
            chat_log.save_to_json()

    @classmethod
    def load_from_json(cls):
        try:
            with open("irclogs_cache/data_store.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            handles = data.get("chat_handles")
            user_ids = set(data.get("user_ids", []))
            user_extra_data = data.get("user_extra_data", {})
        except FileNotFoundError:
            handle_input = input("Enter chat handle(s): ")
            handles = [try_int(handle) for handle in handle_input.split(",")]
            user_ids = set()
            user_extra_data = {}
        data_store = cls(handles, user_ids)
        data_store.user_extra_data = user_extra_data
        return data_store

    async def update_all_logs(self, client):
        for chat_log in self.chat_logs:
            await chat_log.scrape_messages(client)
            for user_id in chat_log.user_ids:
                self.user_ids.add(user_id)

    async def write_all_logs(self, client):
        user_id_lookup = {
            user_id: get_user_name(await client.get_entity(user_id))
            for user_id in self.user_ids
        }
        for chat_log in self.chat_logs:
            chat_entity = await client.get_entity(chat_log.handle)
            chat_name = get_chat_name(chat_entity)
            chat_log.write_log_files(user_id_lookup, chat_name)

    async def write_users_cfg(self, client):
        users_cfg = []
        os.makedirs("pisg_output/user_pics/", exist_ok=True)
        for user_id in tqdm(self.user_ids):
            user_name = get_user_name(await client.get_entity(user_id))
            await client.download_profile_photo(user_id, f"pisg_output/user_pics/{user_id}.png")
            user_data = self.user_extra_data.get(str(user_id), {})
            if "nick" not in user_data:
                user_data["nick"] = user_name
            if "pic" not in user_data:
                user_data["pic"] = f"user_pics/{user_id}.png"
            user_line = "<user " + " ".join(f"{key}=\"{value}\"" for key, value in user_data.items()) + ">"
            users_cfg.append(user_line)
        with open("users.cfg", "w", encoding="utf-8") as f:
            f.write("\n".join(users_cfg))

    async def write_channel_cfg(self, client):
        # Write channel config
        chats_cfg = []
        for chat_handle in self.chat_handles:
            chat_name = get_chat_name(await client.get_entity(chat_handle))
            clean_name = chat_name.replace(" ", r"\ ")
            chats_cfg.append(f"""
        <channel="{chat_name}">
             Logfile = "irclogs/*/{clean_name}*.log"
             OutputFile = "pisg_output/{chat_name}.html"
        </channel>""")
        with open("chats.cfg", "w", encoding="utf-8") as f:
            f.write("\n".join(chats_cfg))


def get_chat_name(entity):
    if hasattr(entity, "title"):
        return f"#{entity.title}"
    else:
        return get_user_name(entity) or str(entity.id)


async def get_message_count(client, entity):
    get_history = GetHistoryRequest(
        peer=entity,
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


class ChatLog:

    def __init__(self, handle: str):
        self.handle = handle
        self.log_entries = defaultdict(lambda: [])  # type: Dict[str, List[LogEntry]]
        self.user_ids = set()  # type: Set[int]
        self.last_message_id = None

    def add_entries(self, log_entries: List["LogEntry"]):
        if log_entries is None:
            return
        for log_entry in log_entries:
            self.log_entries[log_entry.log_datetime.date().isoformat()].append(log_entry)

    async def scrape_messages(self, client):
        entity = await client.get_entity(self.handle)
        count = await get_message_count(client, entity)
        chat_name = get_chat_name(entity)
        latest_id = None
        with tqdm(total=count) as bar:
            async for message in client.iter_messages(entity):
                if latest_id is None:
                    latest_id = message.id
                self.user_ids.add(message.sender.id)
                if self.last_message_id is not None and message.id < self.last_message_id:
                    print(f"- Caught up on {chat_name}")
                    break
                else:
                    self.add_entries(LogEntry.entries_from_message(message, chat_name))
                bar.update(1)
            bar.update(count)
        self.last_message_id = latest_id

    def save_to_json(self):
        os.makedirs("irclogs_cache", exist_ok=True)
        file_name = f"irclogs_cache/{self.handle}.json"
        data = {
            "chat_handle": self.handle,
            "user_ids": list(self.user_ids),
            "last_message_id": self.last_message_id,
            "log_entries": {
                date: [
                    entry.to_json()
                    for entry in self.log_entries[date]
                ] for date in self.log_entries.keys()
            }
        }
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(data, f)

    @classmethod
    def load_from_json(cls, chat_handle):
        file_name = f"irclogs_cache/{chat_handle}.json"
        chat_log = cls(chat_handle)
        try:
            with open(file_name, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return chat_log
        chat_log.last_message_id = data["last_message_id"]
        chat_log.user_ids = set(data["user_ids"])
        for log_date, log_entries in data["log_entries"].items():
            chat_log.log_entries[log_date] = [LogEntry.from_json(log_entry) for log_entry in log_entries]
        return chat_log

    def write_log_files(self, user_id_lookup, chat_name):
        for log_date_str, log_entries in self.log_entries.items():
            log_date = dateutil.parser.parse(log_date_str)
            file_contents = [
                "--- Log opened " + log_date.strftime("%a %b %d 00:00:00 %Y"),
                *[entry.to_log_line(user_id_lookup) for entry in log_entries[::-1]]
            ]
            if log_date.date() != datetime.date.today():
                next_date = log_date + datetime.timedelta(days=1)
                file_contents.append("--- Log closed " + next_date.strftime("%a %b %d 00:00:00 %Y"))
            os.makedirs(f"irclogs/{log_date.year}", exist_ok=True)
            file_name = get_file_name(chat_name, log_date)
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("\n".join(file_contents))


class LogEntry:
    TYPE_TEXT = "TEXT"
    TYPE_JOIN = "JOIN"
    TYPE_QUIT = "QUIT"
    TYPE_ACTION = "ACTION"

    def __init__(self, log_datetime, log_type, user_id, text):
        self.log_datetime = log_datetime
        self.log_type = log_type
        self.user_id = user_id
        self.text = text

    def to_json(self):
        return {
            "datetime": self.log_datetime.isoformat(),
            "entry_type": self.log_type,
            "user_id": self.user_id,
            "text": self.text
        }

    @staticmethod
    def from_json(data):
        return LogEntry(
            dateutil.parser.parse(data["datetime"]),
            data["entry_type"],
            data["user_id"],
            data["text"]
        )

    @classmethod
    def entries_from_message(cls, message, log_name):
        if isinstance(message.action, MessageActionChatDeleteUser):
            return [LogEntry(
                message.date,
                cls.TYPE_QUIT,
                message.sender.id,
                f"[~{message.sender.id}@Telegram] has quit [Left chat]"
            )]
        elif isinstance(message.action, MessageActionChatAddUser):
            return [LogEntry(
                message.date,
                cls.TYPE_JOIN,
                message.sender.id,
                f"[~{message.sender.id}@Telegram] has joined {log_name}"
            )]
        elif message.text:
            return [LogEntry(
                message.date,
                cls.TYPE_TEXT,
                message.sender.id,
                text
            ) for text in message.text.split("\n")[::-1]]
        elif message.media and isinstance(message.media, MessageMediaDocument):
            return [LogEntry(
                message.date,
                cls.TYPE_ACTION,
                message.sender.id,
                f"sent a document ID={message.media.document.id}"
            )]
        elif message.media and isinstance(message.media, MessageMediaPhoto):
            return [LogEntry(
                message.date,
                cls.TYPE_ACTION,
                message.sender.id,
                f"sent a photo ID={message.media.photo.id}"
            )]

    def to_log_line(self, user_id_lookup):
        time = self.log_datetime.time().isoformat()
        user_name = user_id_lookup[self.user_id]
        if self.log_type == self.TYPE_QUIT:
            return f"{time} -!- {user_name} {self.text}"
        elif self.log_type == self.TYPE_JOIN:
            return f"{time} -!- {user_name} {self.text}"
        elif self.log_type == self.TYPE_TEXT:
            return f"{time} < {user_name}> {self.text}"
        elif self.log_type == self.TYPE_ACTION:
            return f"{time} * {user_name} {self.text}"


def try_int(handle):
    try:
        return int(handle.strip())
    except ValueError:
        return handle.strip()


def get_user_name(user):
    full_name = (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)
    if full_name == "":
        return "DELETED_ACCOUNT"
    return full_name.replace(" ", "_")


def get_file_name(log_name, log_date):
    return f"irclogs/{log_date.year}/{log_name}.{log_date.strftime('%m-%d')}.log"


async def update_data(client):
    print("Loading data store")
    data_store = DataStore.load_from_json()
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


def run(conf):
    client = telethon.TelegramClient('log_converter', conf["api_id"], conf["api_hash"])
    client.start()
    client.loop.run_until_complete(update_data(client))


if __name__ == "__main__":
    with open("config.json", "r") as conf_file:
        config = json.load(conf_file)
    run(config)
