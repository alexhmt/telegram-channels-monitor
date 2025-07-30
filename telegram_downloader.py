"""
Улучшенный Telegram Downloader с модульной архитектурой.
Объединяет все функции: фильтрацию, валидацию, экспорт, скачивание медиа и возобновление.
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
import yaml
from datetime import datetime

# Импорт модулей
from auth_info import client
from resume_manager import ResumeManager
from content_filter import ContentFilter
from data_validator import DataValidator
from export_manager import ExportManager
from media_downloader import MediaDownloader
from stream_processor import StreamProcessor

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telegram_downloader.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('telegram_downloader')

class TelegramDownloader:
    """Главный класс для скачивания сообщений из Telegram."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.resume_manager = ResumeManager()
        self.content_filter = ContentFilter(self.config.get('filters', {}))
        self.data_validator = DataValidator(self.config.get('validation', {}))
        self.export_manager = ExportManager(self.config.get('export_dir', 'downloads'))
        self.media_downloader = MediaDownloader(self.config.get('media_dir', 'downloads/media'))
        self.stream_processor = StreamProcessor()
        
        self.batch_size = self.config.get('batch_size', 1000)
        self.max_concurrent_media = self.config.get('max_concurrent_media', 3)
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Загружает конфигурацию из YAML файла."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Файл конфигурации {config_path} не найден, использую настройки по умолчанию")
            return {}
    
    async def list_chats(self) -> List[Dict[str, Any]]:
        """Получает и выводит список всех чатов."""
        logger.info("Получение списка чатов...")
        dialogs = []
        
        print("\n" + "="*80)
        print("СПИСОК ЧАТОВ")
        print("="*80)
        print(f"{'ID':<15} {'Тип':<12} {'Название'}")
        print("-"*80)
        
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            entity_dict = entity.to_dict()
            
            # Определение типа сущности
            from telethon.tl.types import User, Chat, Channel
            if isinstance(entity, User):
                entity_type = "User"
                title = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
                if not title:
                    title = entity.username or f"User{entity.id}"
            elif isinstance(entity, Chat):
                entity_type = "Chat"
                title = entity.title or f"Chat{entity.id}"
            elif isinstance(entity, Channel):
                entity_type = "Channel"
                title = entity.title or f"Channel{entity.id}"
            else:
                entity_type = "Unknown"
                title = f"Chat{entity.id}"
            
            entity_dict["_type"] = entity_type
            entity_dict["title"] = title
            dialogs.append(entity_dict)
            
            # Вывод информации о чате
            print(f"{entity.id:<15} {entity_type:<12} {title}")
        
        print("="*80)
        logger.info(f"Найдено {len(dialogs)} чатов")
        return dialogs
    
    async def find_chat_by_name(self, name: str) -> List[Dict[str, Any]]:
        """Ищет чаты по названию (регистронезависимый поиск)."""
        logger.info(f"Поиск чатов по названию: '{name}'")
        all_chats = await self.list_chats()
        
        # Регистронезависимый поиск
        name_lower = name.lower()
        matching_chats = []
        
        for chat in all_chats:
            chat_title = chat.get('title', '').lower()
            if name_lower in chat_title:
                matching_chats.append(chat)
        
        if matching_chats:
            print(f"\nНайдено {len(matching_chats)} чат(ов) по запросу '{name}':")
            print("-" * 80)
            for chat in all_chats:
                print(f"ID: {chat['id']} | Тип: {chat['_type']} | Название: {chat['title']}")
        else:
            print(f"\nЧаты с названием содержащим '{name}' не найдены")
        
        return matching_chats
    
    async def resolve_chat_identifier(self, identifier: str) -> int:
        """Определяет ID чата из строки (может быть ID или название)."""
        try:
            # Попытка преобразовать в число (ID)
            return int(identifier)
        except ValueError:
            # Поиск по названию
            matching_chats = await self.find_chat_by_name(identifier)
            
            if not matching_chats:
                raise ValueError(f"Чат с названием '{identifier}' не найден")
            
            if len(matching_chats) == 1:
                return matching_chats[0]['id']
            else:
                # Если найдено несколько, выводим список и предлагаем выбрать
                print("\nНайдено несколько чатов:")
                for i, chat in enumerate(matching_chats, 1):
                    print(f"{i}. {chat['title']} (ID: {chat['id']}, Тип: {chat['_type']})")
                
                while True:
                    try:
                        choice = input("\nВыберите номер чата: ").strip()
                        idx = int(choice) - 1
                        if 0 <= idx < len(matching_chats):
                            return matching_chats[idx]['id']
                        else:
                            print("Неверный номер")
                    except (ValueError, IndexError):
                        print("Пожалуйста, введите корректный номер")
    
    async def download_chat(
        self,
        chat_identifier: str,
        days_limit: Optional[int] = None,
        filters: Optional[Dict[str, Any]] = None,
        export_formats: Optional[List[str]] = None,
        download_media: bool = False,
        resume: bool = True
    ) -> Dict[str, Any]:
        """Скачивает сообщения из чата по ID или названию."""
        
        try:
            # Определение ID чата
            chat_id = await self.resolve_chat_identifier(chat_identifier)
            logger.info(f"Начало скачивания чата {chat_identifier} (ID: {chat_id})")
            
            # Получение информации о чате
            entity = await client.get_entity(chat_id)
            chat_title = getattr(entity, 'title', str(chat_id))
            logger.info(f"Чат: {chat_title} (ID: {chat_id})")
        except Exception as e:
            logger.error(f"Не удалось получить информацию о чате: {e}")
            return {'error': str(e)}
        
        # Проверка возобновления
        resume_point = None
        if resume:
            resume_point = await self.resume_manager.check_resume_point(chat_id)
            if resume_point:
                logger.info(f"Найдена точка возобновления: {resume_point} сообщений")
        
        # Подготовка фильтров
        filter_config = filters or self.config.get('filters', {})
        logger.info(f"Фильтры: {self.content_filter.get_filter_summary(filter_config)}")
        
        # Скачивание сообщений
        messages = []
        total_downloaded = 0
        
        # Определение диапазона дат
        offset_date = None
        if days_limit:
            from datetime import timedelta
            offset_date = datetime.now() - timedelta(days=days_limit)
        
        # Создание итератора сообщений
        iterator = client.iter_messages(
            entity,
            limit=None,
            offset_date=offset_date,
            reverse=True  # От старых к новым
        )
        
        # Обработка сообщений пакетами
        batch = []
        async for message in iterator:
            if not message:
                continue
            
            # Проверка точки возобновления
            if resume and resume_point:
                if message.id <= resume_point:
                    continue
            
            # Преобразование сообщения в словарь
            message_dict = message.to_dict()
            
            # Добавление информации об отправителе
            sender_info = {}
            if message.sender:
                sender = message.sender
                sender_info["id"] = sender.id
                from telethon.tl.types import User, Chat, Channel
                if isinstance(sender, User):
                    sender_info["type"] = "User"
                    sender_info["first_name"] = sender.first_name
                    sender_info["last_name"] = sender.last_name
                    sender_info["username"] = sender.username
                elif isinstance(sender, (Chat, Channel)):
                    sender_info["type"] = "Channel"
                    sender_info["title"] = sender.title
            
            message_dict["sender_info"] = sender_info
            
            # Применение фильтров
            if not self.content_filter.apply_filters(message_dict, filter_config):
                continue
            
            batch.append(message_dict)
            
            # Обработка пакета
            if len(batch) >= self.batch_size:
                processed = await self._process_batch(batch, download_media, chat_id)
                messages.extend(processed)
                total_downloaded += len(processed)
                
                # Сохранение прогресса
                if resume:
                    last_message = batch[-1]
                    await self.resume_manager.save_progress(
                        chat_id,
                        last_message['id'],
                        total_downloaded,
                        {"chat_title": chat_title}
                    )
                
                logger.info(f"Обработано {total_downloaded} сообщений")
                batch = []
        
        # Обработка оставшихся сообщений
        if batch:
            processed = await self._process_batch(batch, download_media, chat_id)
            messages.extend(processed)
            total_downloaded += len(processed)
        
        # Валидация и очистка данных
        logger.info("Валидация данных...")
        valid_messages, invalid_messages = self.data_validator.validate_and_clean_batch(messages)
        
        validation_stats = self.data_validator.get_validation_stats(
            len(messages), valid_messages, invalid_messages
        )
        
        logger.info(f"Валидных сообщений: {validation_stats['valid_count']}")
        
        # Экспорт данных
        if valid_messages:
            export_formats = export_formats or self.config.get('export_formats', ['json'])
            logger.info(f"Экспорт в форматы: {', '.join(export_formats)}")
            
            safe_title = "".join(c for c in chat_title if c.isalnum() or c in (' ', '-', '_')).rstrip()
            base_filename = f"{chat_id}_{safe_title}"
            
            export_results = await self.export_manager.export_multiple_formats(
                valid_messages, export_formats, base_filename
            )
            
            # Сохранение отчета о валидации
            self.data_validator.export_validation_report(
                validation_stats,
                f"validation_report_{chat_id}.json"
            )
        
        # Очистка точки возобновления
        if resume:
            await self.resume_manager.mark_completed(chat_id)
        
        return {
            'chat_id': chat_id,
            'chat_title': chat_title,
            'total_messages': total_downloaded,
            'valid_messages': len(valid_messages),
            'invalid_messages': len(invalid_messages),
            'validation_stats': validation_stats,
            'export_files': export_results if valid_messages else {},
            'media_stats': self.media_downloader.get_download_stats() if download_media else None
        }
    
    async def _process_batch(
        self,
        batch: List[Dict[str, Any]],
        download_media: bool,
        chat_id: int = None
    ) -> List[Dict[str, Any]]:
        """Обрабатывает пакет сообщений."""
        if download_media and chat_id:
            # Скачивание медиафайлов для пакета
            try:
                # Получаем оригинальные сообщения для скачивания медиа
                message_ids = [msg['id'] for msg in batch]
                
                # Получаем сущность чата
                entity = await client.get_entity(chat_id)
                
                # Получаем оригинальные сообщения
                original_messages = []
                for msg_id in message_ids:
                    try:
                        msg = await client.get_messages(entity, ids=msg_id)
                        if msg and msg.media:
                            original_messages.append(msg)
                    except Exception as e:
                        logger.warning(f"Не удалось получить сообщение {msg_id}: {e}")
                
                if original_messages:
                    # Скачиваем медиафайлы
                    media_results = await self.media_downloader.download_batch(
                        original_messages,
                        chat_id,
                        max_concurrent=self.max_concurrent_media
                    )
                    
                    # Обновляем сообщения информацией о медиафайлах
                    media_dict = {msg.id: media for msg, media in zip(original_messages, media_results) if media}
                    
                    processed_batch = []
                    for msg_data in batch:
                        msg_id = msg_data['id']
                        if msg_id in media_dict:
                            msg_data['media_info'] = media_dict[msg_id]
                        processed_batch.append(msg_data)
                    
                    return processed_batch
                    
            except Exception as e:
                logger.error(f"Ошибка при скачивании медиафайлов: {e}")
        
        return batch
    
    async def interactive_mode(self):
        """Интерактивный режим работы."""
        print("\n=== Telegram Downloader ===\n")
        
        # Получение списка чатов
        chats = await self.list_chats()
        
        # Выбор чата
        while True:
            chat_input = input("\nВведите ID чата или название (или 'q' для выхода): ").strip()
            if chat_input.lower() == 'q':
                return
            
            try:
                # Попытка определить чат по введенному значению
                chat_id = await self.resolve_chat_identifier(chat_input)
                break
            except ValueError as e:
                print(f"Ошибка: {e}")
        
        # Настройка параметров
        days = input("Скачать сообщения за последние N дней (оставьте пустым для всех): ").strip()
        days_limit = int(days) if days else None
        
        download_media = input("Скачивать медиафайлы? (y/n): ").strip().lower() == 'y'
        
        formats = input("Форматы экспорта (json,csv,excel,html) [json]: ").strip()
        export_formats = [f.strip() for f in formats.split(',')] if formats else ['json']
        
        # Запуск скачивания
        print("\nНачинаю скачивание...")
        result = await self.download_chat(
            chat_identifier=str(chat_id),
            days_limit=days_limit,
            download_media=download_media,
            export_formats=export_formats
        )
        
        print(f"\nСкачивание завершено!")
        print(f"Сообщений: {result['valid_messages']}")
        if result['export_files']:
            print("Файлы:")
            for fmt, path in result['export_files'].items():
                print(f"  {fmt}: {path}")

async def main():
    """Главная функция."""
    parser = argparse.ArgumentParser(description="Улучшенный Telegram Downloader")
    parser.add_argument("--config", default="config.yaml", help="Путь к файлу конфигурации")
    parser.add_argument("--list", action="store_true", help="Показать список чатов")
    parser.add_argument("--chat-id", type=str, help="ID или название чата для скачивания")
    parser.add_argument("--days", type=int, help="Скачать сообщения за последние N дней")
    parser.add_argument("--media", action="store_true", help="Скачивать медиафайлы")
    parser.add_argument("--formats", default="json", help="Форматы экспорта (через запятую)")
    parser.add_argument("--interactive", action="store_true", help="Интерактивный режим")
    
    args = parser.parse_args()
    
    # Инициализация
    downloader = TelegramDownloader(args.config)
    
    try:
        await client.start()
        logger.info("Клиент Telegram запущен")
        
        if args.list:
            chats = await downloader.list_chats()
            return
        
        if args.interactive:
            await downloader.interactive_mode()
            return
        
        if args.chat_id:
            formats = [f.strip() for f in args.formats.split(',')]
            result = await downloader.download_chat(
                chat_identifier=str(args.chat_id),
                days_limit=args.days,
                download_media=args.media,
                export_formats=formats
            )
            print(f"Скачивание завершено: {result}")
        else:
            parser.print_help()
    
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    finally:
        await client.disconnect()
        logger.info("Клиент отключен")

if __name__ == "__main__":
    asyncio.run(main())