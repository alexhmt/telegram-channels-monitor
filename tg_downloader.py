import json
import os
import datetime
import re
import argparse
import asyncio

# Используем ваш модуль для аутентификации
from auth_info import client

# Импорты из Telethon, которые нам понадобятся
from telethon.tl.types import User, Chat, Channel

# --- НОВЫЙ ЭЛЕМЕНТ: ИМЯ ФАЙЛА ДЛЯ КЭША ---
DIALOGS_CACHE_FILE = "dialogs_cache.json"

def json_converter(o):
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    if isinstance(o, bytes):
        return repr(o)

# --- НОВАЯ ФУНКЦИЯ ДЛЯ РАБОТЫ С КЭШЕМ ---
async def update_and_get_dialogs():
    """
    Загружает диалоги из локального кэша, запрашивает свежие данные из Telegram,
    обновляет кэш и сохраняет его на диск. Возвращает полный, актуальный список диалогов.
    """
    cached_dialogs = {}
    
    # 1. Загрузка из локального файла, если он существует
    if os.path.exists(DIALOGS_CACHE_FILE):
        try:
            with open(DIALOGS_CACHE_FILE, 'r', encoding='utf-8') as f:
                cached_dialogs_list = json.load(f)
                # Преобразуем список в словарь для быстрого доступа по ID
                cached_dialogs = {item['id']: item for item in cached_dialogs_list}
            print(f"Загружено {len(cached_dialogs)} диалогов из кэша ({DIALOGS_CACHE_FILE}).")
        except (json.JSONDecodeError, TypeError):
            print(f"Не удалось прочитать файл кэша, будет создан новый.")
            cached_dialogs = {}

    # 2. Запрос свежих данных из Telegram
    print("Запрос свежих данных о диалогах из Telegram...")
    new_dialogs_count = 0
    updated_dialogs_count = 0
    
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        entity_dict = entity.to_dict() # Конвертируем сущность в словарь
        
        # Добавляем тип сущности для удобства
        if isinstance(entity, User):
            entity_dict['_type'] = 'User'
        elif isinstance(entity, Chat):
            entity_dict['_type'] = 'Chat'
        elif isinstance(entity, Channel):
            entity_dict['_type'] = 'Channel'
        
        # 3. Сравнение и обновление
        if entity.id not in cached_dialogs:
            # Это новый диалог, добавляем его
            cached_dialogs[entity.id] = entity_dict
            new_dialogs_count += 1
        else:
            # Диалог уже есть, просто обновляем его данные
            # Это полезно, если изменилось название чата или фото
            if cached_dialogs[entity.id] != entity_dict:
                cached_dialogs[entity.id] = entity_dict
                updated_dialogs_count += 1

    if new_dialogs_count > 0 or updated_dialogs_count > 0:
        print(f"Найдено новых диалогов: {new_dialogs_count}. Обновлено существующих: {updated_dialogs_count}.")
        # 4. Сохранение обновленного списка в файл
        try:
            # Конвертируем словарь обратно в список для сохранения
            all_dialogs_list = list(cached_dialogs.values())
            with open(DIALOGS_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_dialogs_list, f, ensure_ascii=False, indent=4, default=json_converter)
            print(f"Кэш диалогов успешно сохранен в {DIALOGS_CACHE_FILE}.")
        except Exception as e:
            print(f"Ошибка при сохранении кэша: {e}")
    else:
        print("Новых или измененных диалогов не найдено.")

    # Возвращаем список словарей, а не объекты Telethon
    return list(cached_dialogs.values())


# --- ИЗМЕНЕННАЯ ФУНКЦИЯ ---
# Теперь она принимает список диалогов как аргумент
def list_all_chats(dialogs_list):
    """
    Выводит на экран список всех диалогов из предоставленного списка.
    """
    print("-" * 80)
    print(f"{'ID':<15} | {'Тип':<18} | {'Название'}")
    print("-" * 80)

    for entity in dialogs_list:
        entity_type = "Неизвестно"
        title = "N/A"
        
        # Определяем тип из поля '_type', которое мы добавили
        if entity.get('_type') == 'User':
            entity_type = "Пользователь"
            if entity.get('first_name') or entity.get('last_name'):
                title = f"{entity.get('first_name') or ''} {entity.get('last_name') or ''}".strip()
            elif entity.get('deleted'):
                 title = f"Удаленный аккаунт (ID: {entity['id']})"
            else:
                 title = f"Пользователь без имени (ID: {entity['id']})"
        elif entity.get('_type') in ['Chat', 'Channel']:
            entity_type = "Канал/Группа"
            title = entity.get('title')
        
        print(f"{entity['id']:<15} | {entity_type:<18} | {title or 'Без названия'}")

    print("-" * 80)
    print(f"Всего диалогов в кэше: {len(dialogs_list)}")




# Функция скачивания теперь принимает дополнительный параметр days_limit
async def download_chat_history(chat_identifier, days_limit=None):
    """
    Скачивает историю сообщений для указанного чата.
    Если указан days_limit, скачивает сообщения только за последние N дней.
    """
    try:
        print(f"Поиск чата: '{chat_identifier}'...")
        try:
            entity_id = int(chat_identifier)
            entity = await client.get_entity(entity_id)
        except (ValueError, TypeError):
            entity = await client.get_entity(chat_identifier)

        print(f"Чат найден: '{getattr(entity, 'title', entity.id)}' (ID: {entity.id})")
    except ValueError:
        print(f"Ошибка: Чат '{chat_identifier}' не найден.")
        # ... (остальной код обработки ошибок без изменений)
        print("Возможные причины:")
        print("1. Название введено неточно (важны регистр, пробелы и эмодзи).")
        print("2. ID введен неверно (для каналов/супергрупп он должен начинаться с -100).")
        print("3. Вы не являетесь участником этого чата/канала.")
        print("4. Попробуйте запустить с флагом --list для обновления кэша.")
        return
    except Exception as e:
        print(f"Произошла непредвиденная ошибка при поиске чата: {e}")
        return

    all_messages_data = []
    total_messages = 0
    
    # --- ИЗМЕНЕНИЕ НАЧАЛО ---
    offset_date_limit = None
    if days_limit is not None and days_limit > 0:
        # Устанавливаем часовой пояс UTC для корректного сравнения с датами из Telegram
        utc_now = datetime.datetime.now(datetime.timezone.utc)
        offset_date_limit = utc_now - datetime.timedelta(days=days_limit)
        print(f"Установлено ограничение: скачиваем сообщения не старше {days_limit} дней (до {offset_date_limit.strftime('%Y-%m-%d %H:%M')}).")
        # limit=None по-прежнему нужен, т.к. мы не знаем, сколько сообщений было за этот период
        iterator = client.iter_messages(entity, limit=None)
    else:
        print("Скачиваю всю историю сообщений. Это может занять много времени...")
        iterator = client.iter_messages(entity, limit=None)
    # --- ИЗМЕНЕНИЕ КОНЕЦ ---

    async for message in iterator:
        # --- ИЗМЕНЕНИЕ НАЧАЛО ---
        # Если установлено ограничение по дате, и текущее сообщение старше этой даты, прерываем цикл
        if offset_date_limit and message.date < offset_date_limit:
            print("Достигнут лимит по дате. Завершение сканирования.")
            break
        # --- ИЗМЕНЕНИЕ КОНЕЦ ---

        total_messages += 1
        all_messages_data.append(message.to_dict())
        if total_messages % 200 == 0:
            print(f"Скачано {total_messages} сообщений...")
    
    print(f"Скачивание завершено. Всего скачано сообщений: {total_messages}.")

    if not all_messages_data:
        print("В чате нет сообщений для сохранения за указанный период.")
        return

    # --- ИЗМЕНЕНИЕ НАЧАЛО ---
    # Добавляем в имя файла информацию о периоде, если он был задан
    filename_suffix = f"_{days_limit}days" if days_limit else "_full"
    safe_title = re.sub(r'[\\/*?:"<>|]', "", getattr(entity, 'title', f"chat_{entity.id}"))
    filename = f"{entity.id}_{safe_title}{filename_suffix}.json"
    # --- ИЗМЕНЕНИЕ КОНЕЦ ---

    print(f"Сохранение данных в файл: {filename}")
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_messages_data, f, ensure_ascii=False, indent=4, default=json_converter)
        print("Файл успешно сохранен!")
    except Exception as e:
        print(f"Ошибка при сохранении файла: {e}")



async def main():
    """
    Основная функция, которая парсит аргументы командной строки и запускает
    соответствующие действия.
    """
    parser = argparse.ArgumentParser(
        description="Утилита для работы с чатами Telegram с локальным кэшированием.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='Показать список всех доступных чатов, каналов и пользователей из кэша.'
    )
    parser.add_argument(
        '--download',
        metavar='CHAT_ID_OR_NAME',
        help='Скачать историю сообщений указанного чата.'
    )
    # --- ИЗМЕНЕНИЕ НАЧАЛО ---
    parser.add_argument(
        '--days',
        type=int,
        metavar='N',
        help='(Опционально) Скачать сообщения только за последние N дней.\n'
             'Используется вместе с --download. Если не указан, скачивается вся история.'
    )
    # --- ИЗМЕНЕНИЕ КОНЕЦ ---
    parser.add_argument(
        '--force-update',
        action='store_true',
        help='Принудительно обновить кэш диалогов, не выполняя других действий.'
    )

    args = parser.parse_args()
    
    # --- ИЗМЕНЕНИЕ НАЧАЛО ---
    # Проверка, что --days используется только с --download
    if args.days and not args.download:
        parser.error("Аргумент --days можно использовать только вместе с --download.")
    # --- ИЗМЕНЕНИЕ КОНЕЦ ---
        
    await client.start()
    print("Клиент Telegram успешно запущен.")

    all_dialogs = await update_and_get_dialogs()

    if args.list:
        list_all_chats(all_dialogs)
    elif args.download:
        # --- ИЗМЕНЕНИЕ НАЧАЛО ---
        # Передаем значение args.days в функцию скачивания
        await download_chat_history(args.download, args.days)
        # --- ИЗМЕНЕНИЕ КОНЕЦ ---
    elif args.force_update:
        print("Кэш принудительно обновлен. Завершение работы.")
    else:
        parser.print_help()

    await client.disconnect()
    print("Работа клиента завершена.")

if __name__ == "__main__":
    asyncio.run(main())