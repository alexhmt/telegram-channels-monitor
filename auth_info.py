# input info from https://my.telegram.org/
from telethon import TelegramClient, events, sync

# telegram client
session_name = "InfoSystem"
api_id = 22095189
api_hash = "8a587e7e010da9509295a1cbb61ef930"

# telegram bot
bot_token = "6591968387:AAHiIZrDqEPFeX0pHxOyliQACRIukGgBMWM"

client = TelegramClient(session_name, api_id, api_hash, system_version="InfoSystem-0.1")
# client.start()

# get user id of the client
# client_user_id = client.get_me().id
