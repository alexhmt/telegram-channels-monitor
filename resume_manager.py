"""
Модуль для управления возобновлением прерванного скачивания.
Сохраняет прогресс и позволяет продолжить скачивание с места остановки.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
import aiofiles
from datetime import datetime

class ResumeManager:
    """Управляет сохранением и восстановлением прогресса скачивания."""
    
    def __init__(self, resume_dir: str = "resume_data"):
        self.resume_dir = Path(resume_dir)
        self.resume_dir.mkdir(exist_ok=True)
    
    def _get_resume_file(self, chat_id: int) -> Path:
        """Возвращает путь к файлу прогресса для чата."""
        return self.resume_dir / f"resume_{chat_id}.json"
    
    async def save_progress(
        self,
        chat_id: int,
        last_message_id: int,
        total_downloaded: int,
        metadata: Dict[str, Any]
    ) -> None:
        """Сохраняет текущий прогресс скачивания."""
        resume_data = {
            "chat_id": chat_id,
            "last_message_id": last_message_id,
            "total_downloaded": total_downloaded,
            "metadata": metadata,
            "timestamp": datetime.now().isoformat(),
            "status": "in_progress"
        }
        
        resume_file = self._get_resume_file(chat_id)
        async with aiofiles.open(resume_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(resume_data, indent=2, ensure_ascii=False))
    
    async def load_progress(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Загружает сохраненный прогресс для чата."""
        resume_file = self._get_resume_file(chat_id)
        
        if not resume_file.exists():
            return None
        
        try:
            async with aiofiles.open(resume_file, 'r', encoding='utf-8') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            print(f"Ошибка загрузки прогресса: {e}")
            return None
    
    async def mark_completed(self, chat_id: int) -> None:
        """Помечает скачивание как завершенное."""
        resume_file = self._get_resume_file(chat_id)
        if resume_file.exists():
            resume_file.unlink()
    
    async def get_resume_info(self, chat_id: int) -> Optional[Dict[str, Any]]:
        """Возвращает информацию о возможности возобновления."""
        progress = await self.load_progress(chat_id)
        if not progress:
            return None
        
        return {
            "last_message_id": progress["last_message_id"],
            "total_downloaded": progress["total_downloaded"],
            "metadata": progress["metadata"],
            "timestamp": progress["timestamp"]
        }
    
    def list_incomplete_downloads(self) -> list:
        """Возвращает список незавершенных скачиваний."""
        incomplete = []
        for file in self.resume_dir.glob("resume_*.json"):
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    incomplete.append({
                        "chat_id": data["chat_id"],
                        "total_downloaded": data["total_downloaded"],
                        "timestamp": data["timestamp"]
                    })
            except:
                continue
        return incomplete
    
    async def check_resume_point(self, chat_id: int) -> Optional[int]:
        """Проверяет точку возобновления для чата."""
        progress = await self.load_progress(chat_id)
        if progress and "last_message_id" in progress:
            return progress["last_message_id"]
        return None
    
    async def check_resume_point(self, chat_id: int) -> Optional[int]:
        """Проверяет точку возобновления для чата."""
        progress = await self.load_progress(chat_id)
        if progress and "last_message_id" in progress:
            return progress["last_message_id"]
        return None