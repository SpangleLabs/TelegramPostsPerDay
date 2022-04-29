import datetime
from typing import Dict

from telethon.tl.types import MessageActionChatDeleteUser, MessageActionChatAddUser, MessageMediaDocument, \
    MessageMediaPhoto


class LogEntry:
    TYPE_TEXT = "TEXT"
    TYPE_JOIN = "JOIN"
    TYPE_QUIT = "QUIT"
    TYPE_ACTION = "ACTION"

    def __init__(
            self,
            log_datetime: datetime.datetime,
            log_type: str,
            user_id: int,
            text: str,
            message_id: int,
            sub_message_id: int
    ):
        self.log_datetime = log_datetime
        self.log_type = log_type
        self.user_id = user_id
        self.text = text
        self.message_id = message_id
        self.sub_message_id = sub_message_id

    def to_row(self, chat_handle: str) -> Dict:
        return {
            "chat_handle": chat_handle,
            "datetime": self.log_datetime,
            "entry_type": self.log_type,
            "user_id": self.user_id,
            "text": self.text,
            "message_id": self.message_id,
            "sub_message_id": self.sub_message_id
        }

    @staticmethod
    def from_row(row) -> "LogEntry":
        return LogEntry(
            row.datetime,
            row.entry_type,
            row.user_id,
            row.text,
            row.message_id,
            row.sub_message_id
        )

    @classmethod
    def entries_from_message(cls, message, log_name):
        if isinstance(message.action, MessageActionChatDeleteUser):
            return [LogEntry(
                message.date,
                cls.TYPE_QUIT,
                message.sender.id,
                f"[~{message.sender.id}@Telegram] has quit [Left chat]",
                message.id,
                0
            )]
        elif isinstance(message.action, MessageActionChatAddUser):
            return [LogEntry(
                message.date,
                cls.TYPE_JOIN,
                message.sender.id,
                f"[~{message.sender.id}@Telegram] has joined {log_name}",
                message.id,
                0
            )]
        elif message.text:
            return [LogEntry(
                message.date,
                cls.TYPE_TEXT,
                message.sender.id,
                text,
                message.id,
                sub_id
            ) for sub_id, text in enumerate(message.text.split("\n")[::-1])]
        elif message.media and isinstance(message.media, MessageMediaDocument):
            return [LogEntry(
                message.date,
                cls.TYPE_ACTION,
                message.sender.id,
                f"sent a document ID={message.media.document.id}",
                message.id,
                0
            )]
        elif message.media and isinstance(message.media, MessageMediaPhoto):
            return [LogEntry(
                message.date,
                cls.TYPE_ACTION,
                message.sender.id,
                f"sent a photo ID={message.media.photo.id}",
                message.id,
                0
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
