import datetime
import json
import os
from collections import defaultdict

import telethon
import dateutil.parser
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageActionChatDeleteUser, MessageActionChatAddUser, MessageMediaDocument, \
    MessageMediaPhoto
from telethon.tl.types.messages import Messages
from tqdm import tqdm


def try_int(handle):
    try:
        return int(handle.strip())
    except ValueError:
        return handle.strip()


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


def get_user_name(user):
    full_name = (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)
    if full_name == "":
        return "DELETED_ACCOUNT"
    return full_name.replace(" ", "_")


def get_file_name(log_name, log_date):
    return f"irclogs/{log_date.year}/{log_name}.{log_date.strftime('%m-%d')}.log"


def get_log_name(entity):
    return f"#{entity.title}" if hasattr(entity, "title") else (get_user_name(entity) or str(entity.id))


def add_user_to_data(data, user):
    data["users"][user.id] = get_user_name(user)


def add_message_to_log(data, message, log_name):
    add_user_to_data(data, message.sender)
    date = message.date.date().isoformat()
    time = message.date.time().isoformat()
    log_lines = []
    user_name = get_user_name(message.sender)
    if isinstance(message.action, MessageActionChatDeleteUser):
        log_lines = [f"{time} -!- {user_name} [~{message.sender.id}@Telegram] has quit [Left chat]"]
    elif isinstance(message.action, MessageActionChatAddUser):
        log_lines = [f"{time} -!- {user_name} [~{message.sender.id}@Telegram] has joined {log_name}"]
    elif message.text:
        log_lines = [f"{time} < {user_name}> {text}" for text in message.text.split("\n")[::-1]]
    elif message.media and isinstance(message.media, MessageMediaDocument):
        log_lines = [f"{time} * {user_name} sent a document ID={message.media.document.id}"]
    elif message.media and isinstance(message.media, MessageMediaPhoto):
        log_lines = [f"{time} * {user_name} sent a photo ID={message.media.photo.id}"]
    data["log_by_date"][log_name][date] += log_lines


def last_date_for_log(log_name):
    today = datetime.date.today()
    limit = 100
    for x in range(limit):
        log_date = today - datetime.timedelta(days=x)
        file_name = get_file_name(log_name, log_date)
        try:
            with open(file_name, "r", encoding="utf-8") as f:
                last_line = f.readlines()[-1]
                if last_line.startswith("--- Log closed"):
                    return log_date
        except FileNotFoundError:
            continue
    return None


async def parse_messages(client, chat_handles):
    data = {
        "log_by_date": {},
        "users": {},
        "chats": []
    }
    for chat_handle in chat_handles:
        chat_entity = await client.get_entity(chat_handle)
        log_name = get_log_name(chat_entity)
        data["chats"].append(log_name)
        data["log_by_date"][log_name] = defaultdict(lambda: [])
        count = await get_message_count(client, chat_entity)
        last_date = last_date_for_log(log_name)
        # Harvest data
        with tqdm(total=count) as bar:
            async for message in client.iter_messages(chat_entity):
                if last_date is None or message.date.date() > last_date:
                    add_message_to_log(data, message, log_name)
                bar.update(1)
        # Write log files
        for log_date_str, log in data["log_by_date"][log_name].items():
            log_date = dateutil.parser.parse(log_date_str)
            file_contents = [
                "--- Log opened " + log_date.strftime("%a %b %d 00:00:00 %Y"),
                *log[::-1]
            ]
            if log_date.date() != datetime.date.today():
                next_date = log_date + datetime.timedelta(days=1)
                file_contents.append("--- Log closed " + next_date.strftime("%a %b %d 00:00:00 %Y"))
            os.makedirs(f"irclogs/{log_date.year}", exist_ok=True)
            file_name = get_file_name(log_name, log_date)
            with open(file_name, "w", encoding="utf-8") as f:
                f.write("\n".join(file_contents))
    # Download profile photos
    users_cfg = []
    os.makedirs("pisg_output/user_pics/", exist_ok=True)
    for user_id, user_name in data["users"].items():
        await client.download_profile_photo(user_id, f"pisg_output/user_pics/{user_id}.png")
        users_cfg.append(f"<user nick=\"{user_name}\" pic=\"user_pics/{user_id}.png\">")
    with open("users.cfg", "w", encoding="utf-8") as f:
        f.write("\n".join(users_cfg))
    # Write channel config
    chats_cfg = []
    for log_name in data["chats"]:
        clean_name = log_name.replace(" ", r"\ ")
        chats_cfg.append(f"""<channel="{log_name}">
     Logfile = "irclogs/*/{clean_name}*.log"
     OutputFile = "pisg_output/{log_name}.html"
</channel>""")
    with open("chats.cfg", "w", encoding="utf-8") as f:
        f.write("\n".join(chats_cfg))


def run(conf, chat_handles):
    client = telethon.TelegramClient('log_converter', conf["api_id"], conf["api_hash"])
    client.start()
    client.loop.run_until_complete(parse_messages(client, chat_handles))


if __name__ == "__main__":
    with open("config.json", "r") as conf_file:
        config = json.load(conf_file)
    handle_input = input("Enter chat handle(s): ")
    handles = [try_int(handle) for handle in handle_input.split(",")]
    run(config, handles)
