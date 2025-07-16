from telethon.tl.functions.messages import GetDialogsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import InputPeerEmpty
from auth_info import client, bot_token
from bot_send_message import sent_msg2bot, bot_name
import re
import time
from itertools import compress
import json
import os
import datetime

# Start client and get user ID
print("Starting Telegram client...")
client.start()
client_user_id = client.get_me().id
print(f"Client started. User ID: {client_user_id}")

# choose channels to monitor. Set names
monitoring_channels = [
    "ФРИЛАНС | ВАКАНСИИ INSTAGRAM",
    "РЕПЕТИТОРЫ онлайн",
    "Онлайн школы. Чат",
    "Мари про вакансии — инфобизнес",
    "Бизнес-завтрак",
]
# choose key words for fetching messages
key_words = ["бот", "bot", "школа", "работа"]
blocked_ids = [ 875512659, 1308930532, ]

# set time pause for refreshing requests (in seconds)
time_pause = 60

# set limit of messages amount
limit_msg = 10


# preprocess key words
key_words = [x.lower() for x in key_words]
key_words = [re.sub(r"\s+", " ", x) for x in key_words]


def check_key_msg(msg, kw):
    # function for checking if message contains at least one of the key words
    msg = msg.lower()
    msg = re.sub(r"\s+", " ", msg).strip(" ")

    return any(s in msg for s in kw), ", ".join(
        list(compress(kw, [s in msg for s in kw]))
    )


def notify_user(key_word, channel_name, msg_id, chat_id, user_id):
    # function for forwarding message from channel to telegram bot and sending message from bot to the user
    sent_msg2bot(f"key words: {key_word}, channel name: {channel_name}", user_id)
    client.forward_messages(bot_name, msg_id, chat_id)


def main():
    all_msg_file = "all_msg.json"
    if os.path.exists(all_msg_file):
        try:
            with open(all_msg_file, "r") as f:
                all_msg = json.load(f)
            if not isinstance(all_msg, list): # Ensure it's a list
                print("Warning: all_msg.json did not contain a valid list. Starting with an empty list.")
                all_msg = []
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON from {all_msg_file}. Starting with an empty list.")
            all_msg = []
        except Exception as e:
            print(f"Warning: Could not load {all_msg_file} due to {e}. Starting with an empty list.")
            all_msg = []
    else:
        all_msg = []
    
    try:
        while True:
            for target_group in groups:
                if target_group.title in monitoring_channels:
                    try:
                        channel_entity = client.get_entity(target_group.title)
                        posts = client(
                            GetHistoryRequest(
                                peer=channel_entity,
                                limit=limit_msg,
                                offset_date=None,
                                offset_id=0,
                                max_id=0,
                                min_id=0,
                                add_offset=0,
                                hash=0,
                            )
                        )

                        post_msg = posts.messages

                        for m in post_msg:
                            if hasattr(m, 'message') and m.message:
                                print(f"User id: {m.from_id.user_id}")
                                print(f"Сообщение: {m.message[:200]}")
                                is_keyword_present, keywords_found = check_key_msg(m.message, key_words)
                                if is_keyword_present and m.id not in all_msg and m.from_id.user_id not in blocked_ids:
                                    # send message to user with key words and message
                                    notify_user(
                                        keywords_found,
                                        target_group.title,
                                        m.id,
                                        target_group.id,
                                        client_user_id
                                    )
                                    all_msg.append(m.id)
                            else:
                                pass

                        all_msg = all_msg[-500:]

                    except ValueError as e:
                        print(f"Could not find entity for '{target_group.title}': {e}")
                        continue
                    except Exception as e:
                        print(f"An error occurred processing '{target_group.title}': {e}")
                        time.sleep(10)

            time.sleep(time_pause)
    finally:
        print("Saving all_msg to file...")
        try:
            with open(all_msg_file, "w") as f:
                json.dump(all_msg, f)
            print(f"Successfully saved {len(all_msg)} message IDs to {all_msg_file}")
        except Exception as e:
            print(f"Error saving all_msg to {all_msg_file}: {e}")


if __name__ == "__main__":
    print("Fetching dialogs...")
    chats = []
    last_date = None
    groups = []
    try:
        result = client(
            GetDialogsRequest(
                offset_date=last_date,
                offset_id=0,
                offset_peer=InputPeerEmpty(),
                limit=100000,
                hash=0,
            )
        )
        chats.extend(result.chats)
        print(f"Found {len(chats)} chats.")

        # тут значение переменной result сохранить полностью в json в utf-8
        try:
            def default_converter(o):
                if isinstance(o, datetime.datetime):
                    return o.isoformat()
            with open("result.json", "w", encoding="utf-8") as f:
                json.dump(result.to_dict(), f, ensure_ascii=False, indent=4, default=default_converter)
            print("Successfully saved result to result.json")
        except Exception as e:
            print(f"Error saving result to result.json: {e}")
            
        for chat in chats:
            if hasattr(chat, 'title'):
                groups.append(chat)

        print(f"Monitoring {len(groups)} potential groups/channels.")
        main()
    except Exception as e:
        print(f"Failed to get dialogs: {e}")
