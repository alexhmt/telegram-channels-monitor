import json
import os
import datetime
import re
import argparse
import asyncio
import logging
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
import aiofiles

# Используем ваш модуль для аутентификации
from auth_info import client

# Импорты из Telethon, которые нам понадобятся
from telethon.tl.types import User, Chat, Channel
from telethon.errors import (
    FloodWaitError, 
    UserDeactivatedError, 
    SessionPasswordNeededError,
    ChatAdminRequiredError,
    ChannelPrivateError
)

# Импорты для прогресс-бара
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.logging import RichHandler

# Настройка логирования
console = Console()

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """Настройка логирования на основе конфигурации."""
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO').upper())
    
    # Создаем директорию для логов если не существует
    log_file = Path(log_config.get('file', 'logs/downloader.log'))
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Настраиваем форматирование
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Настраиваем логирование в файл
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            RichHandler(console=console, rich_tracebacks=True)
        ]
    )
    
    return logging.getLogger('telegram_downloader')

# Загрузка конфигурации
def load_config() -> Dict[str, Any]:
    """Загружает конфигурацию из YAML файла."""
    config_path = Path('config.yaml')
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}

# Глобальные переменные
CONFIG = load_config()
logger = setup_logging(CONFIG)

# Константы из конфигурации
DIALOGS_CACHE_FILE = CONFIG.get('cache', {}).get('file', 'dialogs_cache.json')
DOWNLOAD_CONFIG = CONFIG.get('download', {})
OUTPUT_CONFIG = CONFIG.get('output', {})

def json_converter(o):
    """Конвертер для сериализации объектов в JSON."""
    if isinstance(o, datetime.datetime):
        return o.isoformat()
    if isinstance(o, bytes):
        return repr(o)

class TelegramDownloader:
    """Класс для управления скачиванием данных из Telegram."""
    
    def __init__(self, client, config: Dict[str, Any]):
        self.client = client
        self.config = config
        self.logger = logging.getLogger('telegram_downloader')
        
    async def update_and_get_dialogs(self) -> List[Dict[str, Any]]:
        """
        Загружает диалоги из локального кэша, запрашивает свежие данные из Telegram,
        обновляет кэш и сохраняет его на диск.
        """
        cached_dialogs = {}
        
        # Загрузка из локального файла
        if os.path.exists(DIALOGS_CACHE_FILE):
            try:
                async with aiofiles.open(DIALOGS_CACHE_FILE, "r", encoding="utf-8") as f:
                    content = await f.read()
                    cached_dialogs_list = json.loads(content)
                    cached_dialogs = {item["id"]: item for item in cached_dialogs_list}
                self.logger.info(f"Загружено {len(cached_dialogs)} диалогов из кэша")
            except (json.JSONDecodeError, TypeError) as e:
                self.logger.warning(f"Не удалось прочитать файл кэша: {e}")
                cached_dialogs = {}
            except Exception as e:
                self.logger.error(f"Ошибка при чтении кэша: {e}")
                cached_dialogs = {}

        # Запрос свежих данных из Telegram
        self.logger.info("Запрос свежих данных о диалогах из Telegram...")
        new_dialogs_count = 0
        updated_dialogs_count = 0
        
        try:
            async for dialog in self.client.iter_dialogs():
                entity = dialog.entity
                entity_dict = entity.to_dict()
                
                # Добавляем тип сущности
                if isinstance(entity, User):
                    entity_dict["_type"] = "User"
                elif isinstance(entity, Chat):
                    entity_dict["_type"] = "Chat"
                elif isinstance(entity, Channel):
                    entity_dict["_type"] = "Channel"
                
                # Сравнение и обновление
                if entity.id not in cached_dialogs:
                    cached_dialogs[entity.id] = entity_dict
                    new_dialogs_count += 1
                else:
                    if cached_dialogs[entity.id] != entity_dict:
                        cached_dialogs[entity.id] = entity_dict
                        updated_dialogs_count += 1
                        
        except FloodWaitError as e:
            self.logger.error(f"Flood wait error: нужно подождать {e.seconds} секунд")
            raise
        except Exception as e:
            self.logger.error(f"Ошибка при получении диалогов: {e}")
            raise
            
        # Сохранение обновленного кэша
        if new_dialogs_count > 0 or updated_dialogs_count > 0:
            self.logger.info(f"Найдено новых диалогов: {new_dialogs_count}, обновлено: {updated_dialogs_count}")
            try:
                all_dialogs_list = list(cached_dialogs.values())
                async with aiofiles.open(DIALOGS_CACHE_FILE, "w", encoding="utf-8") as f:
                    await f.write(json.dumps(
                        all_dialogs_list,
                        ensure_ascii=False,
                        indent=2,
                        default=json_converter
                    ))
                self.logger.info("Кэш диалогов успешно сохранен")
            except Exception as e:
                self.logger.error(f"Ошибка при сохранении кэша: {e}")
        
        return list(cached_dialogs.values())
    
    def list_all_chats(self, dialogs_list: List[Dict[str, Any]]) -> None:
        """Выводит список всех диалогов."""
        console.print("\n[bold cyan]Список всех диалогов:[/bold cyan]")
        console.print("-" * 80)
        console.print(f"{'ID':<15} | {'Тип':<18} | {'Название'}")
        console.print("-" * 80)
        
        for entity in dialogs_list:
            entity_type = "Неизвестно"
            title = "N/A"
            
            if entity.get("_type") == "User":
                entity_type = "Пользователь"
                if entity.get("first_name") or entity.get("last_name"):
                    title = f"{entity.get('first_name') or ''} {entity.get('last_name') or ''}".strip()
                elif entity.get("deleted"):
                    title = f"Удаленный аккаунт"
                else:
                    title = f"Пользователь без имени"
            elif entity.get("_type") in ["Chat", "Channel"]:
                entity_type = "Канал/Группа"
                title = entity.get("title", "Без названия")
            
            console.print(f"{entity['id']:<15} | {entity_type:<18} | {title}")
        
        console.print("-" * 80)
        console.print(f"Всего диалогов: {len(dialogs_list)}")
    
    async def download_chat_history(
        self, 
        chat_identifier: str, 
        days_limit: Optional[int] = None,
        chunk_size: int = 1000
    ) -> None:
        """
        Скачивает историю сообщений для указанного чата с прогресс-баром.
        """
        try:
            # Поиск чата
            self.logger.info(f"Поиск чата: '{chat_identifier}'")
            try:
                entity_id = int(chat_identifier)
                entity = await self.client.get_entity(entity_id)
            except (ValueError, TypeError):
                entity = await self.client.get_entity(chat_identifier)
            
            chat_title = getattr(entity, 'title', str(entity.id))
            self.logger.info(f"Чат найден: '{chat_title}' (ID: {entity.id})")
            
        except ValueError as e:
            self.logger.error(f"Чат '{chat_identifier}' не найден: {e}")
            console.print(f"[red]Ошибка: Чат '{chat_identifier}' не найден.[/red]")
            console.print("Используйте --list для просмотра доступных чатов.")
            return
        except Exception as e:
            self.logger.error(f"Ошибка при поиске чата: {e}")
            console.print(f"[red]Ошибка при поиске чата: {e}[/red]")
            return
        
        # Настройка параметров скачивания
        offset_date_limit = None
        if days_limit is not None and days_limit > 0:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            offset_date_limit = utc_now - datetime.timedelta(days=days_limit)
            self.logger.info(f"Ограничение по дате: {days_limit} дней")
        
        # Подготовка имени файла
        filename_suffix = f"_{days_limit}days" if days_limit else "_full"
        safe_title = re.sub(
            r'[\\/*?:"<>|]', "", 
            getattr(entity, "title", f"chat_{entity.id}")
        )
        
        output_dir = Path(OUTPUT_CONFIG.get('directory', 'downloads'))
        output_dir.mkdir(exist_ok=True)
        
        filename = output_dir / f"{entity.id}_{safe_title}{filename_suffix}.json"
        
        # Скачивание сообщений с прогресс-баром
        all_messages_data = []
        total_messages = 0
        
        try:
            # Получаем общее количество сообщений для прогресс-бара
            message_count = await self.client.get_messages(entity, limit=1)
            total_count = message_count.total if message_count else None
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                
                task = progress.add_task(
                    f"Скачивание сообщений из {chat_title}...", 
                    total=total_count
                )
                
                iterator = self.client.iter_messages(entity, limit=None)
                
                async for message in iterator:
                    if offset_date_limit and message.date < offset_date_limit:
                        self.logger.info("Достигнут лимит по дате")
                        break
                    
                    # Преобразуем сообщение
                    message_dict = message.to_dict()
                    
                    # Добавляем информацию об отправителе
                    sender_info = {}
                    if message.sender:
                        sender = message.sender
                        sender_info["id"] = sender.id
                        if isinstance(sender, User):
                            sender_info["type"] = "User"
                            sender_info["first_name"] = sender.first_name
                            sender_info["last_name"] = sender.last_name
                            sender_info["username"] = sender.username
                        elif isinstance(sender, (Chat, Channel)):
                            sender_info["type"] = "Channel"
                            sender_info["title"] = sender.title
                    
                    message_dict["sender_info"] = sender_info
                    all_messages_data.append(message_dict)
                    total_messages += 1
                    
                    # Обновляем прогресс
                    progress.update(task, advance=1)
                    
                    # Периодическое сохранение для больших объемов
                    if len(all_messages_data) >= chunk_size:
                        self.logger.debug(f"Обработано {total_messages} сообщений")
        
        except FloodWaitError as e:
            self.logger.error(f"Flood wait: {e.seconds} секунд")
            console.print(f"[red]Ошибка: нужно подождать {e.seconds} секунд[/red]")
            return
        except ChatAdminRequiredError:
            self.logger.error("Требуются права администратора")
            console.print("[red]Ошибка: требуются права администратора для скачивания[/red]")
            return
        except ChannelPrivateError:
            self.logger.error("Приватный канал или вы не являетесь участником")
            console.print("[red]Ошибка: приватный канал или вы не являетесь участником[/red]")
            return
        except Exception as e:
            self.logger.error(f"Ошибка при скачивании сообщений: {e}")
            console.print(f"[red]Ошибка при скачивании: {e}[/red]")
            return
        
        # Сохранение результатов
        if not all_messages_data:
            self.logger.warning("Нет сообщений для сохранения")
            console.print("[yellow]В чате нет сообщений для сохранения за указанный период.[/yellow]")
            return
        
        try:
            async with aiofiles.open(filename, "w", encoding="utf-8") as f:
                await f.write(json.dumps(
                    all_messages_data,
                    ensure_ascii=False,
                    indent=2,
                    default=json_converter
                ))
            
            file_size = filename.stat().st_size
            self.logger.info(f"Файл сохранен: {filename} ({file_size / 1024 / 1024:.2f} MB)")
            console.print(f"[green]✓[/green] Файл успешно сохранен: [bold]{filename}[/bold]")
            console.print(f"  Сообщений: {total_messages}, Размер: {file_size / 1024 / 1024:.2f} MB")
            
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении файла: {e}")
            console.print(f"[red]Ошибка при сохранении файла: {e}[/red]")

async def main():
    """Основная функция."""
    parser = argparse.ArgumentParser(
        description="Улучшенная утилита для работы с чатами Telegram",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="Показать список всех доступных чатов, каналов и пользователей из кэша.",
    )
    
    parser.add_argument(
        "--download",
        metavar="CHAT_ID_OR_NAME",
        help="Скачать историю сообщений указанного чата.",
    )
    
    parser.add_argument(
        "--days",
        type=int,
        metavar="N",
        help="Скачать сообщения только за последние N дней.\n"
             "Используется вместе с --download.",
    )
    
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Принудительно обновить кэш диалогов.",
    )
    
    parser.add_argument(
        "--config",
        metavar="PATH",
        help="Путь к файлу конфигурации (по умолчанию: config.yaml)",
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Включить подробное логирование (DEBUG уровень)",
    )
    
    args = parser.parse_args()
    
    # Проверка аргументов
    if args.days and not args.download:
        parser.error("Аргумент --days можно использовать только вместе с --download.")
    
    # Настройка уровня логирования
    if args.verbose:
        logging.getLogger('telegram_downloader').setLevel(logging.DEBUG)
    
    # Инициализация клиента
    downloader = TelegramDownloader(client, CONFIG)
    
    try:
        await client.start()
        logger.info("Клиент Telegram успешно запущен")
        console.print("[green]✓[/green] Клиент Telegram успешно запущен")
        
        # Обновление кэша
        all_dialogs = await downloader.update_and_get_dialogs()
        
        # Выполнение запрошенных действий
        if args.list:
            downloader.list_all_chats(all_dialogs)
        elif args.download:
            await downloader.download_chat_history(args.download, args.days)
        elif args.force_update:
            logger.info("Кэш обновлен по запросу")
            console.print("[green]✓[/green] Кэш диалогов обновлен")
        else:
            parser.print_help()
            
    except SessionPasswordNeededError:
        logger.error("Требуется двухфакторная аутентификация")
        console.print("[red]Ошибка: требуется двухфакторная аутентификация[/red]")
    except UserDeactivatedError:
        logger.error("Аккаунт деактивирован")
        console.print("[red]Ошибка: аккаунт деактивирован[/red]")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        console.print(f"[red]Критическая ошибка: {e}[/red]")
    finally:
        await client.disconnect()
        logger.info("Клиент отключен")
        console.print("[green]✓[/green] Работа завершена")

if __name__ == "__main__":
    asyncio.run(main())