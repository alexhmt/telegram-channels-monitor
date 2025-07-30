"""
Улучшенный Telegram Downloader с потоковой обработкой, прогресс-барами и модульной архитектурой.
"""

import json
import os
import datetime
import re
import argparse
import asyncio
import logging
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional, AsyncGenerator
import aiofiles

# Импорты для Telegram
from auth_info import client
from telethon.tl.types import User, Chat, Channel
from telethon.errors import (
    FloodWaitError, 
    UserDeactivatedError, 
    SessionPasswordNeededError,
    ChatAdminRequiredError,
    ChannelPrivateError
)

# Импорты для UI
from rich.console import Console
from rich.progress import (
    Progress, 
    SpinnerColumn, 
    TextColumn, 
    BarColumn, 
    TaskProgressColumn,
    TimeRemainingColumn,
    MofNCompleteColumn
)
from rich.logging import RichHandler
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

# Импорты модулей
from stream_processor import StreamProcessor, MemoryMonitor

# Настройка логирования
console = Console()

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """Настройка логирования на основе конфигурации."""
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO').upper())
    
    log_file = Path(log_config.get('file', 'logs/downloader.log'))
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
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

class TelegramDownloader:
    """Основной класс для скачивания данных из Telegram."""
    
    def __init__(self, client, config: Dict[str, Any]):
        self.client = client
        self.config = config
        self.logger = logging.getLogger('telegram_downloader')
        self.stream_processor = StreamProcessor(
            chunk_size=config.get('processing', {}).get('chunk_size', 1000),
            max_memory_mb=config.get('processing', {}).get('max_memory_mb', 100)
        )
        self.memory_monitor = MemoryMonitor(
            threshold_mb=config.get('processing', {}).get('memory_threshold_mb', 500)
        )
        
    def json_converter(self, o):
        """Конвертер для сериализации объектов в JSON."""
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        if isinstance(o, bytes):
            return repr(o)
    
    async def update_and_get_dialogs(self) -> List[Dict[str, Any]]:
        """Обновляет и возвращает список диалогов с прогресс-баром."""
        cache_file = self.config.get('cache', {}).get('file', 'dialogs_cache.json')
        cached_dialogs = {}
        
        # Загрузка из кэша
        if os.path.exists(cache_file):
            try:
                async with aiofiles.open(cache_file, "r", encoding="utf-8") as f:
                    content = await f.read()
                    cached_dialogs_list = json.loads(content)
                    cached_dialogs = {item["id"]: item for item in cached_dialogs_list}
                self.logger.info(f"Загружено {len(cached_dialogs)} диалогов из кэша")
            except Exception as e:
                self.logger.warning(f"Ошибка загрузки кэша: {e}")
                cached_dialogs = {}
        
        # Обновление диалогов
        self.logger.info("Обновление списка диалогов...")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            task = progress.add_task("Получение диалогов...", total=None)
            
            try:
                new_dialogs_count = 0
                updated_dialogs_count = 0
                
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
                    
                    # Обновляем кэш
                    if entity.id not in cached_dialogs:
                        cached_dialogs[entity.id] = entity_dict
                        new_dialogs_count += 1
                    else:
                        if cached_dialogs[entity.id] != entity_dict:
                            cached_dialogs[entity.id] = entity_dict
                            updated_dialogs_count += 1
                
                progress.update(task, description="Сохранение кэша...")
                
                # Сохраняем кэш
                if new_dialogs_count > 0 or updated_dialogs_count > 0:
                    all_dialogs_list = list(cached_dialogs.values())
                    async with aiofiles.open(cache_file, "w", encoding="utf-8") as f:
                        await f.write(json.dumps(
                            all_dialogs_list,
                            ensure_ascii=False,
                            indent=2,
                            default=self.json_converter
                        ))
                
                progress.update(task, description=f"Обновлено: {new_dialogs_count} новых, {updated_dialogs_count} изменено")
                
            except Exception as e:
                self.logger.error(f"Ошибка обновления диалогов: {e}")
                raise
        
        return list(cached_dialogs.values())
    
    def display_chats_table(self, dialogs_list: List[Dict[str, Any]]) -> None:
        """Отображает список диалогов в виде красивой таблицы."""
        table = Table(title="Доступные диалоги")
        
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Тип", style="magenta")
        table.add_column("Название", style="green")
        table.add_column("Участников", justify="right", style="yellow")
        
        for entity in dialogs_list:
            entity_type = entity.get("_type", "Неизвестно")
            title = "Без названия"
            
            if entity_type == "User":
                title = f"{entity.get('first_name', '')} {entity.get('last_name', '')}".strip()
                if not title:
                    title = f"User_{entity.get('id', 'unknown')}"
            elif entity_type in ["Chat", "Channel"]:
                title = entity.get("title", "Без названия")
            
            participants = entity.get("participants_count", "-")
            
            table.add_row(
                str(entity.get("id", "N/A")),
                entity_type,
                title,
                str(participants)
            )
        
        console.print(table)
        console.print(f"\n[bold]Всего диалогов: {len(dialogs_list)}[/bold]")
    
    async def create_messages_generator(
        self,
        entity,
        days_limit: Optional[int] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Создает асинхронный генератор сообщений."""
        
        offset_date_limit = None
        if days_limit is not None and days_limit > 0:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            offset_date_limit = utc_now - datetime.timedelta(days=days_limit)
        
        iterator = self.client.iter_messages(entity, limit=None)
        
        async for message in iterator:
            if offset_date_limit and message.date < offset_date_limit:
                break
            
            # Преобразуем сообщение
            message_dict = message.to_dict()
            
            # Добавляем информацию об отправителе
            sender_info = {}
            if message.sender:
                sender = message.sender
                sender_info["id"] = sender.id
                sender_info["type"] = type(sender).__name__
                
                if isinstance(sender, User):
                    sender_info["first_name"] = sender.first_name
                    sender_info["last_name"] = sender.last_name
                    sender_info["username"] = sender.username
                elif isinstance(sender, (Chat, Channel)):
                    sender_info["title"] = sender.title
            
            message_dict["sender_info"] = sender_info
            
            yield message_dict
            
            # Проверяем использование памяти
            await self.memory_monitor.force_gc_if_needed()
    
    async def download_chat_history(
        self,
        chat_identifier: str,
        days_limit: Optional[int] = None,
        use_streaming: bool = True
    ) -> None:
        """Скачивает историю сообщений с улучшенным UI и потоковой обработкой."""
        
        try:
            # Поиск чата
            self.logger.info(f"Поиск чата: '{chat_identifier}'")
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                
                task = progress.add_task("Поиск чата...", total=None)
                
                try:
                    entity_id = int(chat_identifier)
                    entity = await self.client.get_entity(entity_id)
                except (ValueError, TypeError):
                    entity = await self.client.get_entity(chat_identifier)
                
                chat_title = getattr(entity, 'title', str(entity.id))
                progress.update(task, description=f"Чат найден: {chat_title}")
        
        except Exception as e:
            self.logger.error(f"Чат не найден: {e}")
            console.print(Panel(
                f"[red]Ошибка: Чат '{chat_identifier}' не найден.[/red]\n"
                f"Используйте --list для просмотра доступных чатов.",
                title="Ошибка",
                border_style="red"
            ))
            return
        
        # Настройка вывода
        output_config = self.config.get('output', {})
        output_dir = Path(output_config.get('directory', 'downloads'))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        filename_suffix = f"_{days_limit}days" if days_limit else "_full"
        safe_title = re.sub(
            r'[\\/*?:"<>|]', "",
            getattr(entity, "title", f"chat_{entity.id}")
        )
        
        output_file = output_dir / f"{entity.id}_{safe_title}{filename_suffix}.json"
        
        # Получаем количество сообщений
        try:
            message_count = await self.client.get_messages(entity, limit=1)
            total_messages = message_count.total if message_count else None
        except:
            total_messages = None
        
        # Скачивание сообщений
        console.print(Panel(
            f"Начинаю скачивание сообщений из чата:\n"
            f"[bold cyan]{chat_title}[/bold cyan]\n"
            f"Файл: [dim]{output_file}[/dim]\n"
            f"Ограничение по дням: [yellow]{days_limit or 'нет'}[/yellow]",
            title="Скачивание",
            border_style="blue"
        ))
        
        try:
            if use_streaming:
                # Используем потоковую обработку
                messages_gen = self.create_messages_generator(entity, days_limit)
                
                with Progress(
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TimeRemainingColumn(),
                    console=console
                ) as progress:
                    
                    task = progress.add_task(
                        "Скачивание сообщений...",
                        total=total_messages
                    )
                    
                    stats = await self.stream_processor.process_messages_stream(
                        messages_gen,
                        output_file,
                        batch_size=100
                    )
                    
                    console.print(Panel(
                        f"[green]✓ Скачивание завершено![/green]\n\n"
                        f"Сообщений: [bold]{stats['total_messages']}[/bold]\n"
                        f"Размер файла: [bold]{stats['file_size'] / 1024 / 1024:.2f} MB[/bold]\n"
                        f"Время: [bold]{stats['processing_time']:.2f} секунд[/bold]",
                        title="Результат",
                        border_style="green"
                    ))
            
            else:
                # Классический способ (для совместимости)
                await self._classic_download(entity, output_file, days_limit, total_messages)
        
        except Exception as e:
            self.logger.error(f"Ошибка при скачивании: {e}")
            console.print(Panel(
                f"[red]Ошибка при скачивании: {e}[/red]",
                title="Ошибка",
                border_style="red"
            ))
    
    async def _classic_download(
        self,
        entity,
        output_file: Path,
        days_limit: Optional[int],
        total_messages: Optional[int]
    ) -> None:
        """Классический способ скачивания для совместимости."""
        
        offset_date_limit = None
        if days_limit is not None and days_limit > 0:
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            offset_date_limit = utc_now - datetime.timedelta(days=days_limit)
        
        all_messages = []
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Скачивание сообщений...", total=total_messages)
            
            async for message in self.client.iter_messages(entity, limit=None):
                if offset_date_limit and message.date < offset_date_limit:
                    break
                
                message_dict = message.to_dict()
                
                sender_info = {}
                if message.sender:
                    sender = message.sender
                    sender_info["id"] = sender.id
                    sender_info["type"] = type(sender).__name__
                    
                    if isinstance(sender, User):
                        sender_info["first_name"] = sender.first_name
                        sender_info["last_name"] = sender.last_name
                        sender_info["username"] = sender.username
                    elif isinstance(sender, (Chat, Channel)):
                        sender_info["title"] = sender.title
                
                message_dict["sender_info"] = sender_info
                all_messages.append(message_dict)
                
                progress.update(task, advance=1)
        
        # Сохранение
        async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(
                all_messages,
                ensure_ascii=False,
                indent=2,
                default=self.json_converter
            ))
        
        console.print(Panel(
            f"[green]✓ Скачивание завершено![/green]\n\n"
            f"Сообщений: [bold]{len(all_messages)}[/bold]\n"
            f"Размер файла: [bold]{output_file.stat().st_size / 1024 / 1024:.2f} MB[/bold]",
            title="Результат",
            border_style="green"
        ))

async def main():
    """Основная функция приложения."""
    
    parser = argparse.ArgumentParser(
        description="Улучшенный Telegram Downloader",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="Показать список всех доступных чатов"
    )
    
    parser.add_argument(
        "--download",
        metavar="CHAT_ID_OR_NAME",
        help="Скачать историю сообщений указанного чата"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        metavar="N",
        help="Скачать сообщения только за последние N дней"
    )
    
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="Принудительно обновить кэш диалогов"
    )
    
    parser.add_argument(
        "--classic",
        action="store_true",
        help="Использовать классический способ скачивания (без потоковой обработки)"
    )
    
    parser.add_argument(
        "--config",
        metavar="PATH",
        help="Путь к конфигурационному файлу"
    )
    
    args = parser.parse_args()
    
    # Проверка аргументов
    if args.days and not args.download:
        parser.error("Аргумент --days можно использовать только с --download")
    
    # Инициализация
    console.print(Panel(
        "[bold cyan]Telegram Downloader v2.0[/bold cyan]\n"
        "Улучшенный инструмент для скачивания сообщений из Telegram",
        border_style="cyan"
    ))
    
    try:
        await client.start()
        console.print("[green]✓ Клиент Telegram успешно запущен[/green]")
        
        downloader = TelegramDownloader(client, CONFIG)
        
        # Обновляем диалоги
        all_dialogs = await downloader.update_and_get_dialogs()
        
        # Выполняем запрошенное действие
        if args.list:
            downloader.display_chats_table(all_dialogs)
        elif args.download:
            await downloader.download_chat_history(
                args.download,
                args.days,
                use_streaming=not args.classic
            )
        elif args.force_update:
            console.print("[green]✓ Кэш диалогов обновлен[/green]")
        else:
            parser.print_help()
    
    except Exception as e:
        console.print(Panel(
            f"[red]Критическая ошибка: {e}[/red]",
            title="Ошибка",
            border_style="red"
        ))
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    
    finally:
        await client.disconnect()
        console.print("[dim]Работа клиента завершена[/dim]")

if __name__ == "__main__":
    asyncio.run(main())