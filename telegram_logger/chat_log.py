import datetime
import os
from typing import Optional, List, TYPE_CHECKING

from tqdm import tqdm

from telegram_logger.telegram_utils import get_chat_name, get_message_count
from telegram_logger.log_entry import LogEntry

if TYPE_CHECKING:
    from telegram_logger.database import Database


def get_file_name(log_name, log_date):
    return f"irclogs/{log_date.year}/{log_name}.{log_date.strftime('%m-%d')}.log"


class ChatLog:

    def __init__(self, handle: str, db: "Database", last_message_id: Optional[int] = None):
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
    def load_from_database(cls, chat_handle: str, database: "Database") -> "ChatLog":
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


