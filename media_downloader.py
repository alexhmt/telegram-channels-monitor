"""
Модуль для скачивания медиафайлов из сообщений Telegram.
Поддерживает фото, видео, аудио, документы и другие типы медиа.
"""

import os
import aiofiles
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Callable
import asyncio
from datetime import datetime
import mimetypes
import hashlib
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument, MessageMediaContact,
    MessageMediaGeo, MessageMediaVenue, MessageMediaGame,
    DocumentAttributeFilename, DocumentAttributeVideo,
    DocumentAttributeAudio, DocumentAttributeImageSize
)
import logging

class MediaDownloader:
    """Скачивание медиафайлов из сообщений Telegram."""
    
    def __init__(self, download_dir: str = "downloads/media"):
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger('telegram_downloader.media')
        
        # Поддиректории для разных типов медиа
        self.subdirs = {
            'photo': self.download_dir / 'photos',
            'video': self.download_dir / 'videos',
            'audio': self.download_dir / 'audio',
            'document': self.download_dir / 'documents',
            'voice': self.download_dir / 'voice',
            'sticker': self.download_dir / 'stickers',
            'contact': self.download_dir / 'contacts',
            'location': self.download_dir / 'locations'
        }
        
        for subdir in self.subdirs.values():
            subdir.mkdir(exist_ok=True)
    
    def _generate_filename(
        self,
        media_type: str,
        original_filename: Optional[str] = None,
        extension: Optional[str] = None,
        message_id: int = 0
    ) -> str:
        """Генерирует безопасное имя файла на основе ID сообщения."""
        # Используем только ID сообщения как имя файла
        filename = str(message_id)
        
        # Определение расширения
        if extension:
            filename += extension
        elif original_filename and '.' in original_filename:
            # Извлекаем расширение из оригинального имени файла
            ext = '.' + original_filename.rsplit('.', 1)[-1].lower()
            filename += ext
        else:
            filename += self._get_default_extension(media_type)
        
        return filename
    
    def _get_default_extension(self, media_type: str) -> str:
        """Возвращает расширение по умолчанию для типа медиа."""
        extensions = {
            'photo': '.jpg',
            'video': '.mp4',
            'audio': '.mp3',
            'voice': '.ogg',
            'sticker': '.webp',
            'document': '.bin'
        }
        return extensions.get(media_type, '.bin')
    
    def _get_file_extension_from_mime(self, mime_type: str) -> str:
        """Определяет расширение по MIME типу."""
        extension = mimetypes.guess_extension(mime_type)
        return extension or '.bin'
    
    async def download_media(
        self,
        message,
        chat_id: int,
        progress_callback: Optional[Callable] = None
    ) -> Optional[Dict[str, Any]]:
        """Скачивает медиафайл из сообщения."""
        try:
            if not message.media:
                return None
            
            media_info = await self._extract_media_info(message)
            if not media_info:
                return None
            
            # Определение пути для сохранения
            save_dir = self.subdirs[media_info['type']]
            filename = self._generate_filename(
                media_info['type'],
                media_info.get('filename'),
                media_info.get('extension'),
                message.id
            )
            file_path = save_dir / filename
            
            # Проверка существующего файла
            if file_path.exists():
                file_hash = await self._calculate_file_hash(file_path)
                return {
                    'type': media_info['type'],
                    'file_path': str(file_path),
                    'file_size': file_path.stat().st_size,
                    'file_hash': file_hash,
                    'downloaded': False,
                    'reason': 'already_exists'
                }
            
            # Скачивание файла
            downloaded_path = await message.download_media(
                file=str(file_path),
                progress_callback=progress_callback
            )
            
            if downloaded_path:
                file_path = Path(downloaded_path)
                file_hash = await self._calculate_file_hash(file_path)
                
                return {
                    'type': media_info['type'],
                    'file_path': str(file_path),
                    'file_size': file_path.stat().st_size,
                    'file_hash': file_hash,
                    'downloaded': True,
                    'metadata': media_info.get('metadata', {})
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Ошибка скачивания медиа: {e}")
            return None
    
    async def _extract_media_info(self, message) -> Optional[Dict[str, Any]]:
        """Извлекает информацию о медиафайле из сообщения."""
        media = message.media
        
        if isinstance(media, MessageMediaPhoto):
            return {
                'type': 'photo',
                'extension': '.jpg',
                'metadata': {
                    'date': media.photo.date,
                    'sizes': len(media.photo.sizes)
                }
            }
        
        elif isinstance(media, MessageMediaDocument):
            document = media.document
            mime_type = document.mime_type
            
            # Определение типа по MIME
            if mime_type.startswith('image/'):
                media_type = 'photo'
            elif mime_type.startswith('video/'):
                media_type = 'video'
            elif mime_type.startswith('audio/'):
                if 'voice' in mime_type or document.attributes:
                    for attr in document.attributes:
                        if isinstance(attr, DocumentAttributeAudio) and attr.voice:
                            media_type = 'voice'
                            break
                    else:
                        media_type = 'audio'
                else:
                    media_type = 'audio'
            elif mime_type.startswith('application/'):
                media_type = 'document'
            else:
                media_type = 'document'
            
            # Извлечение имени файла
            filename = None
            for attr in document.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    filename = attr.file_name
                    break
            
            extension = self._get_file_extension_from_mime(mime_type)
            
            metadata = {
                'mime_type': mime_type,
                'size': document.size,
                'date': document.date
            }
            
            # Дополнительная информация для видео
            for attr in document.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    metadata.update({
                        'duration': attr.duration,
                        'width': attr.w,
                        'height': attr.h
                    })
                elif isinstance(attr, DocumentAttributeAudio):
                    metadata.update({
                        'duration': attr.duration,
                        'title': attr.title,
                        'performer': attr.performer
                    })
                elif isinstance(attr, DocumentAttributeImageSize):
                    metadata.update({
                        'width': attr.w,
                        'height': attr.h
                    })
            
            return {
                'type': media_type,
                'filename': filename,
                'extension': extension,
                'metadata': metadata
            }
        
        elif isinstance(media, MessageMediaContact):
            return {
                'type': 'contact',
                'metadata': {
                    'phone_number': media.phone_number,
                    'first_name': media.first_name,
                    'last_name': media.last_name,
                    'user_id': media.user_id
                }
            }
        
        elif isinstance(media, MessageMediaGeo):
            return {
                'type': 'location',
                'metadata': {
                    'latitude': media.geo.lat,
                    'longitude': media.geo.long
                }
            }
        
        return None
    
    async def _calculate_file_hash(self, file_path: Path) -> str:
        """Вычисляет MD5 хэш файла."""
        hash_md5 = hashlib.md5()
        async with aiofiles.open(file_path, 'rb') as f:
            async for chunk in self._read_file_chunks(f):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    async def _read_file_chunks(self, file, chunk_size: int = 8192):
        """Читает файл по частям."""
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            yield chunk
    
    async def download_batch(
        self,
        messages: List[Any],
        chat_id: int,
        max_concurrent: int = 3
    ) -> List[Dict[str, Any]]:
        """Скачивает медиафайлы из пакета сообщений."""
        semaphore = asyncio.Semaphore(max_concurrent)
        results = []
        
        async def download_with_semaphore(message):
            async with semaphore:
                return await self.download_media(message, chat_id)
        
        tasks = [download_with_semaphore(msg) for msg in messages if msg.media]
        downloaded = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in downloaded:
            if isinstance(result, dict):
                results.append(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Ошибка при скачивании: {result}")
        
        return results
    
    def get_download_stats(self) -> Dict[str, Any]:
        """Возвращает статистику скачивания."""
        stats = {
            'total_files': 0,
            'total_size': 0,
            'by_type': {}
        }
        
        for media_type, subdir in self.subdirs.items():
            if subdir.exists():
                files = list(subdir.iterdir())
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                
                stats['by_type'][media_type] = {
                    'count': len(files),
                    'size': total_size
                }
                stats['total_files'] += len(files)
                stats['total_size'] += total_size
        
        return stats
    
    async def cleanup_duplicates(self) -> Dict[str, int]:
        """Удаляет дубликаты медиафайлов по хэшу."""
        removed_count = 0
        hash_map = {}
        
        for subdir in self.subdirs.values():
            if not subdir.exists():
                continue
            
            for file_path in subdir.iterdir():
                if not file_path.is_file():
                    continue
                
                file_hash = await self._calculate_file_hash(file_path)
                
                if file_hash in hash_map:
                    # Найден дубликат
                    file_path.unlink()
                    removed_count += 1
                    self.logger.info(f"Удален дубликат: {file_path.name}")
                else:
                    hash_map[file_hash] = str(file_path)
        
        return {'removed_duplicates': removed_count}