import datetime
from typing import List, Optional, Set

import sqlalchemy

from telegram_logger.chat_log import ChatLog
from telegram_logger.log_entry import LogEntry


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
            sqlalchemy.Column("message_id", sqlalchemy.Integer()),
            sqlalchemy.Column("sub_message_id", sqlalchemy.Integer()),
            sqlalchemy.Column("text", sqlalchemy.Text()),
            sqlalchemy.UniqueConstraint("chat_handle", "message_id", "sub_message_id")
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
            sqlalchemy.asc(self.log_entries.columns.message_id),
            sqlalchemy.asc(self.log_entries.columns.sub_message_id)
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
