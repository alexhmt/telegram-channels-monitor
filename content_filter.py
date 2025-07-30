"""
Модуль для фильтрации сообщений по типам контента.
Поддерживает фильтрацию по типам медиа, текстовым паттернам и другим критериям.
"""

import re
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime

class ContentFilter:
    """Фильтрация сообщений по различным критериям."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.filters = {
            'media_types': self._filter_by_media_type,
            'text_patterns': self._filter_by_text_patterns,
            'date_range': self._filter_by_date_range,
            'sender_ids': self._filter_by_sender,
            'min_length': self._filter_by_length,
            'has_links': self._filter_by_links,
            'has_mentions': self._filter_by_mentions,
            'is_forwarded': self._filter_by_forwarded,
            'is_edited': self._filter_by_edited
        }
    
    def _filter_by_media_type(self, message: Dict[str, Any], allowed_types: List[str]) -> bool:
        """Фильтрация по типу медиа."""
        if not allowed_types:
            return True
        
        media_types = {
            'text': bool(message.get('message')),
            'photo': bool(message.get('photo')),
            'video': bool(message.get('video')),
            'audio': bool(message.get('audio')),
            'document': bool(message.get('document')),
            'sticker': bool(message.get('sticker')),
            'voice': bool(message.get('voice')),
            'video_note': bool(message.get('video_note')),
            'contact': bool(message.get('contact')),
            'location': bool(message.get('geo')),
            'poll': bool(message.get('poll')),
            'game': bool(message.get('game'))
        }
        
        return any(media_types.get(media_type, False) for media_type in allowed_types)
    
    def _filter_by_text_patterns(self, message: Dict[str, Any], patterns: List[str]) -> bool:
        """Фильтрация по текстовым паттернам (регулярные выражения)."""
        if not patterns:
            return True
        
        text = message.get('message', '')
        if not text:
            return False
        
        return any(re.search(pattern, text, re.IGNORECASE) for pattern in patterns)
    
    def _filter_by_date_range(self, message: Dict[str, Any], date_range: Dict[str, str]) -> bool:
        """Фильтрация по диапазону дат."""
        if not date_range:
            return True
        
        message_date = datetime.fromisoformat(message.get('date', '').replace('Z', '+00:00'))
        
        start_date = None
        end_date = None
        
        if 'start' in date_range:
            start_date = datetime.fromisoformat(date_range['start'])
        if 'end' in date_range:
            end_date = datetime.fromisoformat(date_range['end'])
        
        if start_date and message_date < start_date:
            return False
        if end_date and message_date > end_date:
            return False
        
        return True
    
    def _filter_by_sender(self, message: Dict[str, Any], sender_ids: List[int]) -> bool:
        """Фильтрация по ID отправителя."""
        if not sender_ids:
            return True
        
        sender_info = message.get('sender_info', {})
        return sender_info.get('id') in sender_ids
    
    def _filter_by_length(self, message: Dict[str, Any], min_length: int) -> bool:
        """Фильтрация по минимальной длине текста."""
        if not min_length:
            return True
        
        text = message.get('message', '')
        return len(text) >= min_length
    
    def _filter_by_links(self, message: Dict[str, Any], has_links: bool) -> bool:
        """Фильтрация по наличию ссылок."""
        if has_links is None:
            return True
        
        text = message.get('message', '')
        url_pattern = r'https?://\S+|www\.\S+'
        has_url = bool(re.search(url_pattern, text))
        
        return has_url == has_links
    
    def _filter_by_mentions(self, message: Dict[str, Any], has_mentions: bool) -> bool:
        """Фильтрация по наличию упоминаний."""
        if has_mentions is None:
            return True
        
        text = message.get('message', '')
        mention_pattern = r'@\w+'
        has_mention = bool(re.search(mention_pattern, text))
        
        return has_mention == has_mentions
    
    def _filter_by_forwarded(self, message: Dict[str, Any], is_forwarded: bool) -> bool:
        """Фильтрация по пересланным сообщениям."""
        if is_forwarded is None:
            return True
        
        fwd_from = message.get('fwd_from')
        return bool(fwd_from) == is_forwarded
    
    def _filter_by_edited(self, message: Dict[str, Any], is_edited: bool) -> bool:
        """Фильтрация по редактированным сообщениям."""
        if is_edited is None:
            return True
        
        edit_date = message.get('edit_date')
        return bool(edit_date) == is_edited
    
    def apply_filters(self, message: Dict[str, Any], filters_config: Dict[str, Any]) -> bool:
        """Применяет все активные фильтры к сообщению."""
        for filter_name, filter_config in filters_config.items():
            if filter_name in self.filters:
                if not self.filters[filter_name](message, filter_config):
                    return False
        
        return True
    
    def create_filter_config(self, **kwargs) -> Dict[str, Any]:
        """Создает конфигурацию фильтров из параметров."""
        config = {}
        
        if 'media_types' in kwargs:
            config['media_types'] = kwargs['media_types']
        
        if 'text_patterns' in kwargs:
            config['text_patterns'] = kwargs['text_patterns']
        
        if 'date_range' in kwargs:
            config['date_range'] = kwargs['date_range']
        
        if 'sender_ids' in kwargs:
            config['sender_ids'] = kwargs['sender_ids']
        
        if 'min_length' in kwargs:
            config['min_length'] = kwargs['min_length']
        
        if 'has_links' in kwargs:
            config['has_links'] = kwargs['has_links']
        
        if 'has_mentions' in kwargs:
            config['has_mentions'] = kwargs['has_mentions']
        
        if 'is_forwarded' in kwargs:
            config['is_forwarded'] = kwargs['is_forwarded']
        
        if 'is_edited' in kwargs:
            config['is_edited'] = kwargs['is_edited']
        
        return config
    
    def get_filter_summary(self, filters_config: Dict[str, Any]) -> str:
        """Возвращает текстовое описание активных фильтров."""
        if not filters_config:
            return "Фильтры не заданы"
        
        summary_parts = []
        
        if 'media_types' in filters_config:
            summary_parts.append(f"Типы медиа: {', '.join(filters_config['media_types'])}")
        
        if 'text_patterns' in filters_config:
            summary_parts.append(f"Текстовые паттерны: {len(filters_config['text_patterns'])} шт.")
        
        if 'date_range' in filters_config:
            date_range = filters_config['date_range']
            range_str = []
            if 'start' in date_range:
                range_str.append(f"с {date_range['start']}")
            if 'end' in date_range:
                range_str.append(f"до {date_range['end']}")
            summary_parts.append(f"Диапазон дат: {' '.join(range_str)}")
        
        if 'sender_ids' in filters_config:
            summary_parts.append(f"Отправители: {len(filters_config['sender_ids'])} ID")
        
        if 'min_length' in filters_config:
            summary_parts.append(f"Мин. длина: {filters_config['min_length']} символов")
        
        if filters_config.get('has_links') is not None:
            summary_parts.append("Со ссылками" if filters_config['has_links'] else "Без ссылок")
        
        if filters_config.get('has_mentions') is not None:
            summary_parts.append("С упоминаниями" if filters_config['has_mentions'] else "Без упоминаний")
        
        if filters_config.get('is_forwarded') is not None:
            summary_parts.append("Пересланные" if filters_config['is_forwarded'] else "Оригинальные")
        
        if filters_config.get('is_edited') is not None:
            summary_parts.append("Отредактированные" if filters_config['is_edited'] else "Неотредактированные")
        
        return "; ".join(summary_parts)