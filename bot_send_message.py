import telebot
from auth_info import bot_token

bot = telebot.TeleBot(bot_token)
bot_name = f"@{bot.get_me().username}"


def sent_msg2bot(txt, user_id):
    """Sends the given text message to the specified user ID via the bot."""
    try:
        # Directly send the message, no need for handlers here
        bot.send_message(user_id, txt)
        # print(f"Sent to {user_id}: {txt}") # Optional: for debugging
    except Exception as e:
        print(f"Error sending message via bot to {user_id}: {e}")
