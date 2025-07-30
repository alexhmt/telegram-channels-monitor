"""
Модуль для потоковой обработки больших объемов данных Telegram.
Оптимизирует использование памяти при скачивании больших чатов.
"""

import json
import asyncio
import aiofiles
from pathlib import Path
from typing import AsyncGenerator, Dict, Any, Optional, List
import logging
from datetime import datetime

logger = logging.getLogger('telegram_downloader')

class StreamProcessor:
    """Класс для потоковой обработки сообщений Telegram."""
    
    def __init__(self, chunk_size: int = 1000, max_memory_mb: int = 100):
        self.chunk_size = chunk_size
        self.max_memory_mb = max_memory_mb
        self.logger = logging.getLogger('telegram_downloader')
        
    async def process_messages_stream(
        self,
        messages_generator: AsyncGenerator[Dict[str, Any], None],
        output_file: Path,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """
        Потоковая обработка сообщений с сохранением в файл.
        
        Args:
            messages_generator: Асинхронный генератор сообщений
            output_file: Путь к выходному файлу
            batch_size: Размер пакета для записи
            
        Returns:
            Статистика обработки
        """
        stats = {
            'total_messages': 0,
            'file_size': 0,
            'processing_time': 0,
            'chunks_written': 0
        }
        
        start_time = datetime.now()
        
        try:
            # Создаем директорию если не существует
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Открываем файл для записи
            async with aiofiles.open(output_file, 'w', encoding='utf-8') as f:
                # Записываем начало JSON массива
                await f.write('[\n')
                
                first_message = True
                batch = []
                
                async for message in messages_generator:
                    batch.append(message)
                    stats['total_messages'] += 1
                    
                    # Записываем пакет когда достигли размера
                    if len(batch) >= batch_size:
                        await self._write_batch(f, batch, first_message)
                        stats['chunks_written'] += 1
                        first_message = False
                        batch = []
                        
                        # Логируем прогресс
                        if stats['total_messages'] % 1000 == 0:
                            self.logger.info(f"Обработано {stats['total_messages']} сообщений")
                
                # Записываем оставшиеся сообщения
                if batch:
                    await self._write_batch(f, batch, first_message)
                
                # Закрываем JSON массив
                await f.write('\n]')
                
            # Обновляем статистику
            stats['file_size'] = output_file.stat().st_size
            stats['processing_time'] = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(
                f"Потоковая обработка завершена: "
                f"{stats['total_messages']} сообщений, "
                f"{stats['file_size'] / 1024 / 1024:.2f} MB за "
                f"{stats['processing_time']:.2f} секунд"
            )
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Ошибка в потоковой обработке: {e}")
            raise
    
    async def _write_batch(
        self, 
        file_handle, 
        batch: List[Dict[str, Any]], 
        is_first: bool
    ) -> None:
        """Записывает пакет сообщений в файл."""
        for i, message in enumerate(batch):
            if not is_first or i > 0:
                await file_handle.write(',\n')
            
            # Минифицируем JSON для экономии места
            json_str = json.dumps(message, ensure_ascii=False, separators=(',', ':'))
            await file_handle.write('  ' + json_str)
    
    async def create_memory_efficient_generator(
        self,
        messages_iterator,
        max_memory_items: int = 1000
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Создает генератор с ограничением памяти.
        
        Args:
            messages_iterator: Итератор сообщений из Telethon
            max_memory_items: Максимальное количество сообщений в памяти
            
        Yields:
            Сообщения по одному
        """
        buffer = []
        
        async for message in messages_iterator:
            # Преобразуем сообщение в словарь
            message_dict = message.to_dict()
            
            # Добавляем информацию об отправителе
            sender_info = {}
            if message.sender:
                sender = message.sender
                sender_info["id"] = sender.id
                sender_info["type"] = type(sender).__name__
                
                if hasattr(sender, 'first_name'):
                    sender_info["first_name"] = sender.first_name
                    sender_info["last_name"] = sender.last_name
                    sender_info["username"] = sender.username
                elif hasattr(sender, 'title'):
                    sender_info["title"] = sender.title
            
            message_dict["sender_info"] = sender_info
            
            yield message_dict
            
            # Ограничиваем размер буфера
            buffer.append(message_dict)
            if len(buffer) > max_memory_items:
                buffer.pop(0)
    
    async def split_large_export(
        self,
        messages_generator: AsyncGenerator[Dict[str, Any], None],
        output_dir: Path,
        max_file_size_mb: int = 50,
        file_prefix: str = "messages"
    ) -> List[Path]:
        """
        Разбивает большой экспорт на несколько файлов.
        
        Args:
            messages_generator: Генератор сообщений
            output_dir: Директория для сохранения
            max_file_size_mb: Максимальный размер файла в MB
            file_prefix: Префикс имени файла
            
        Returns:
            Список созданных файлов
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        current_file_num = 1
        current_file_size = 0
        current_messages = []
        created_files = []
        
        max_bytes = max_file_size_mb * 1024 * 1024
        
        async for message in messages_generator:
            message_json = json.dumps(message, ensure_ascii=False)
            message_size = len(message_json.encode('utf-8'))
            
            # Если добавление сообщения превысит лимит, сохраняем текущий файл
            if current_messages and (current_file_size + message_size) > max_bytes:
                file_path = output_dir / f"{file_prefix}_part{current_file_num}.json"
                await self._save_chunk(current_messages, file_path)
                created_files.append(file_path)
                
                current_file_num += 1
                current_messages = []
                current_file_size = 0
            
            current_messages.append(message)
            current_file_size += message_size
        
        # Сохраняем последний файл
        if current_messages:
            file_path = output_dir / f"{file_prefix}_part{current_file_num}.json"
            await self._save_chunk(current_messages, file_path)
            created_files.append(file_path)
        
        return created_files
    
    async def _save_chunk(self, messages: List[Dict[str, Any]], file_path: Path) -> None:
        """Сохраняет чанк сообщений в файл."""
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(messages, ensure_ascii=False, indent=2))
        
        self.logger.info(f"Сохранен файл: {file_path} ({len(messages)} сообщений)")
    
    async def compress_old_exports(self, directory: Path, days_old: int = 7) -> List[Path]:
        """
        Сжимает старые экспортные файлы для экономии места.
        
        Args:
            directory: Директория с файлами
            days_old: Сжимать файлы старше N дней
            
        Returns:
            Список сжатых файлов
        """
        import gzip
        import time
        
        compressed_files = []
        cutoff_time = time.time() - (days_old * 24 * 3600)
        
        for file_path in directory.glob("*.json"):
            if file_path.stat().st_mtime < cutoff_time:
                compressed_path = file_path.with_suffix('.json.gz')
                
                # Сжимаем файл
                async with aiofiles.open(file_path, 'rb') as f_in:
                    content = await f_in.read()
                
                with gzip.open(compressed_path, 'wb') as f_out:
                    f_out.write(content.encode('utf-8'))
                
                # Удаляем оригинал
                file_path.unlink()
                
                compressed_files.append(compressed_path)
                self.logger.info(f"Сжат файл: {file_path} -> {compressed_path}")
        
        return compressed_files

class MemoryMonitor:
    """Мониторинг использования памяти."""
    
    def __init__(self, threshold_mb: int = 500):
        self.threshold_mb = threshold_mb
        self.logger = logging.getLogger('telegram_downloader')
    
    def get_memory_usage(self) -> float:
        """Получает текущее использование памяти в MB."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0
    
    async def check_memory_threshold(self) -> bool:
        """Проверяет превышение порога памяти."""
        current_usage = self.get_memory_usage()
        
        if current_usage > self.threshold_mb:
            self.logger.warning(
                f"Использование памяти превысило порог: "
                f"{current_usage:.2f} MB > {self.threshold_mb} MB"
            )
            return True
        
        return False
    
    async def force_gc_if_needed(self) -> None:
        """Принудительный сбор мусора при необходимости."""
        import gc
        
        if await self.check_memory_threshold():
            gc.collect()
            self.logger.info("Выполнен принудительный сбор мусора")