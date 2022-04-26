import asyncio
import datetime
import json
import os
import sys
from typing import Optional, List, Set, Dict, Union

import sqlalchemy
import telethon
import dateutil.parser
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageActionChatDeleteUser, MessageActionChatAddUser, MessageMediaDocument, \
    MessageMediaPhoto, InputPeerChannel
from telethon.tl.types.messages import Messages
from tqdm import tqdm


class Database:
    def __init__(self, db_str: str) -> None:
        self.engine = sqlalchemy.create_engine(db_str)
        self.conn = self.engine.connect()
        self.metadata = sqlalchemy.MetaData()
        self.chat_logs = sqlalchemy.Table(
            "telepisg_chat_logs",
            self.metadata,
            sqlalchemy.Column("chat_handle", sqlalchemy.String(), nullable=False, primary_key=True),
            sqlalchemy.Column("last_message_id", sqlalchemy.Integer())
        )
        self.log_entries = sqlalchemy.Table(
            "telepisg_log_entries",
            self.metadata,
            sqlalchemy.Column(
                "chat_handle",
                sqlalchemy.String(),
                sqlalchemy.ForeignKey(
                    "telepisg_chat_logs.chat_handle",
                    ondelete="CASCADE"
                ),
                nullable=False
            ),
            sqlalchemy.Column("datetime", sqlalchemy.DateTime()),
            sqlalchemy.Column("entry_type", sqlalchemy.String()),
            sqlalchemy.Column("user_id", sqlalchemy.Integer()),
            sqlalchemy.Column("text", sqlalchemy.Text())
        )
        self.metadata.create_all(self.engine)

    def insert_log_entries(self, chat_handle: str, log_entries: List["LogEntry"]):
        query = sqlalchemy.insert(self.log_entries)
        values_list = [
            log_entry.to_row(chat_handle) for log_entry in log_entries
        ]
        self.conn.execute(query, values_list)

    def list_log_dates(self, chat_handle: str) -> List[datetime.date]:
        cols = [sqlalchemy.cast(self.log_entries.columns.datetime, sqlalchemy.Date)]
        if self.engine.url.drivername == "sqlite":
            cols = [sqlalchemy.func.DATE(self.log_entries.columns.datetime).label("datetime")]
        query = sqlalchemy.select(
            cols
        ).distinct(
        ).where(
            self.log_entries.columns.chat_handle == chat_handle
        ).order_by(
            sqlalchemy.asc(self.log_entries.columns.datetime)
        )
        result = self.conn.execute(query)
        rows = result.fetchall()
        if self.engine.url.drivername == "sqlite":
            return [
                datetime.date.fromisoformat(row.datetime) for row in rows
            ]
        return [
            row.datetime for row in rows
        ]

    def list_log_entries(self, chat_handle: str, log_date: Optional[datetime.date]):
        conditions = [
            self.log_entries.columns.chat_handle == chat_handle
        ]
        if log_date:
            start_datetime = datetime.datetime.combine(log_date, datetime.time(0, 0, 0))
            end_datetime = start_datetime + datetime.timedelta(days=1)
            conditions.extend([
                self.log_entries.columns.datetime >= start_datetime,
                self.log_entries.columns.datetime < end_datetime
            ])
        query = sqlalchemy.select(
            self.log_entries.columns
        ).where(
            sqlalchemy.and_(*conditions)
        ).order_by(
            sqlalchemy.asc(self.log_entries.columns.datetime)
        )
        result = self.conn.execute(query)
        return [
            LogEntry.from_row(row)
            for row in result.fetchall()
        ]

    def list_user_ids(self, chat_handle: str) -> Set[int]:
        query = sqlalchemy.select(
            self.log_entries.columns.user_id
        ).distinct(
            self.log_entries.columns.user_id
        ).where(
            self.log_entries.columns.chat_handle == chat_handle
        )
        result = self.conn.execute(query)
        return set([row.user_id for row in result.fetchall()])

    def update_chat_log(self, chat_handle: str, last_message_id: Optional[int]):
        query = sqlalchemy.update(
            self.chat_logs
        ).values(
            last_message_id=last_message_id
        ).where(
            self.chat_logs.columns.chat_handle == chat_handle
        )
        self.conn.execute(query)

    def create_chat_log(self, chat_handle: str) -> None:
        query = sqlalchemy.insert(
            self.chat_logs
        ).values(
            chat_handle=chat_handle,
            last_message_id=None
        )
        self.conn.execute(query)

    def get_chat_log(self, chat_handle: str) -> "ChatLog":
        query = sqlalchemy.select(
            self.chat_logs.columns.last_message_id
        ).where(
            self.chat_logs.columns.chat_handle == chat_handle
        )
        result = self.conn.execute(query)
        row = result.fetchone()
        if row is None:
            self.create_chat_log(chat_handle)
            return ChatLog(chat_handle, self)
        return ChatLog(chat_handle, self, last_message_id=row.last_message_id)


class DataStore:

    def __init__(self, db: Database, chat_handles: Optional[List] = None, user_ids: Optional[Set] = None):
        self.db = db
        self.chat_handles = chat_handles or []
        self.user_ids = user_ids or set()
        self.chat_logs = [ChatLog.load_from_database(chat_handle, self.db) for chat_handle in self.chat_handles]
        self.user_extra_data = {}

    def add_chat(self, chat_handle):
        self.chat_handles.append(chat_handle)
        self.chat_logs.append(ChatLog.load_from_database(chat_handle, self.db))

    def save_to_json(self):
        os.makedirs("irclogs_cache", exist_ok=True)
        with open("irclogs_cache/data_store.json", "w", encoding="utf-8") as f:
            json.dump({
                "chat_handles": self.chat_handles,
                "user_ids": list(self.user_ids),
                "user_extra_data": self.user_extra_data
            }, f, indent=2)

    @classmethod
    def load_from_json(cls, db: Database):
        try:
            with open("irclogs_cache/data_store.json", "r", encoding="utf-8") as f:
                data = json.load(f)
            handles = data.get("chat_handles")
            user_ids = set(data.get("user_ids", []))
            user_extra_data = data.get("user_extra_data", {})
        except FileNotFoundError:
            handles = []
            user_ids = set()
            user_extra_data = {}
        data_store = cls(db, handles, user_ids)
        data_store.user_extra_data = user_extra_data
        return data_store

    async def update_all_logs(self, client):
        for chat_log in self.chat_logs:
            await chat_log.scrape_messages(client)
            for user_id in self.db.list_user_ids(chat_log.handle):
                self.user_ids.add(user_id)

    async def write_all_logs(self, client):
        user_id_lookup = {
            user_id: get_user_name_unique_deleted(await client.get_entity(user_id))
            for user_id in self.user_ids
        }
        for chat_log in tqdm(self.chat_logs):
            chat_entity = await client.get_entity(chat_log.handle)
            chat_name = get_chat_name(chat_entity)
            chat_log.write_log_files(user_id_lookup, chat_name)

    async def write_users_cfg(self, client):
        users_cfg = []
        os.makedirs("pisg_output/user_pics/", exist_ok=True)
        deleted_account_count = 0
        for user_id in tqdm(self.user_ids):
            user_name = get_user_name(await client.get_entity(user_id))
            pic = await client.download_profile_photo(user_id, f"pisg_output/user_pics/{user_id}.png")
            user_data = self.user_extra_data.get(str(user_id), {})
            if user_name == "DELETED_ACCOUNT":
                deleted_account_count += 1
                if "alias" not in user_data:
                    user_data["alias"] = get_user_name_unique_deleted(await client.get_entity(user_id))
                if "nick" not in user_data:
                    user_data["nick"] = f"{user_name}{deleted_account_count}"
            else:
                if "nick" not in user_data:
                    user_data["nick"] = user_name
            if "pic" not in user_data and pic is not None:
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


async def get_message_count(client, entity, latest_id=0):
    get_history = GetHistoryRequest(
        peer=entity,
        offset_id=0,
        offset_date=None,
        add_offset=0,
        limit=1,
        max_id=0,
        min_id=latest_id or 0,
        hash=0
    )
    history = await client(get_history)
    if isinstance(history, Messages):
        count = len(history.messages)
    else:
        count = history.count
    return count


class ChatLog:

    def __init__(self, handle: str, db: Database, last_message_id: Optional[int] = None):
        self.handle = handle
        self.db = db
        self.last_message_id = last_message_id

    def add_entries(self, log_entries: List["LogEntry"]):
        if log_entries is None:
            return
        self.db.insert_log_entries(self.handle, log_entries)

    async def scrape_messages(self, client):
        entity = await client.get_entity(self.handle)
        count = await get_message_count(client, entity, self.last_message_id)
        chat_name = get_chat_name(entity)
        latest_id = None
        print(f"- Updating {chat_name} logs")
        with tqdm(total=count) as bar:
            async for message in client.iter_messages(entity):
                if latest_id is None:
                    latest_id = message.id
                if self.last_message_id is not None and message.id <= self.last_message_id:
                    print(f"- Caught up on {chat_name}")
                    break
                else:
                    self.add_entries(LogEntry.entries_from_message(message, chat_name))
                bar.update(1)
        self.last_message_id = latest_id
        self.db.update_chat_log(self.handle, self.last_message_id)

    @classmethod
    def load_from_database(cls, chat_handle: str, database: Database) -> "ChatLog":
        return database.get_chat_log(chat_handle)

    def write_log_files(self, user_id_lookup, chat_name):
        for log_date in self.db.list_log_dates(self.handle):
            file_contents = [
                "--- Log opened " + log_date.strftime("%a %b %d 00:00:00 %Y"),
                *[entry.to_log_line(user_id_lookup) for entry in self.db.list_log_entries(self.handle, log_date)]
            ]
            if log_date != datetime.date.today():
                next_date = log_date + datetime.timedelta(days=1)
                file_contents.append("--- Log closed " + next_date.strftime("%a %b %d 00:00:00 %Y"))
            os.makedirs(f"irclogs/{log_date.year}", exist_ok=True)
            file_name = get_file_name(chat_name, log_date)
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("\n".join(file_contents))


def decode_log_entry(data: Dict) -> Union["LogEntry", Dict]:
    if all(key in data for key in ["datetime", "entry_type", "user_id", "text"]):
        return LogEntry.from_json(data)
    return data


def encode_log_entry(obj):
    if isinstance(obj, LogEntry):
        return obj.to_json()
    raise TypeError(f"{obj} is not JSON serializable")


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

    def to_row(self, chat_handle: str) -> Dict:
        return {
            "chat_handle": chat_handle,
            "datetime": self.log_datetime,
            "entry_type": self.log_type,
            "user_id": self.user_id,
            "text": self.text
        }

    @staticmethod
    def from_row(row) -> "LogEntry":
        return LogEntry(
            row.datetime,
            row.entry_type,
            row.user_id,
            row.text
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


def get_user_name_unique_deleted(user):
    full_name = (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)
    if full_name == "":
        return f"DELETED_ACCOUNT{user.id}"
    return full_name.replace(" ", "_")


def get_file_name(log_name, log_date):
    return f"irclogs/{log_date.year}/{log_name}.{log_date.strftime('%m-%d')}.log"


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
