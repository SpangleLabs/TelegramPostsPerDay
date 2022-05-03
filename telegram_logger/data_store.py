import json
import os
from typing import Optional, List, Set, TYPE_CHECKING

from tqdm import tqdm

from telegram_logger.chat_log import ChatLog
from telegram_logger.telegram_utils import get_chat_name, get_user_name, get_user_name_unique_deleted

if TYPE_CHECKING:
    from telegram_logger.database import Database


class DataStore:

    def __init__(self, db: "Database", chat_handles: Optional[List] = None, user_ids: Optional[Set] = None):
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
    def load_from_json(cls, db: "Database"):
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
