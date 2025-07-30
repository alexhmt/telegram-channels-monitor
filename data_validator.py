"""
Модуль для валидации и очистки данных сообщений.
Обеспечивает качество экспортируемых данных.
"""

import re
import html
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import logging

class DataValidator:
    """Валидация и очистка данных сообщений."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.logger = logging.getLogger('telegram_downloader.validator')
        
        # Настройки валидации
        self.max_text_length = self.config.get('max_text_length', 10000)
        self.min_text_length = self.config.get('min_text_length', 0)
        self.remove_empty_messages = self.config.get('remove_empty_messages', True)
        self.remove_deleted_users = self.config.get('remove_deleted_users', True)
        self.validate_dates = self.config.get('validate_dates', True)
        self.clean_html = self.config.get('clean_html', True)
        self.remove_duplicates = self.config.get('remove_duplicates', True)
    
    def validate_message(self, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Валидирует одно сообщение."""
        try:
            # Проверка обязательных полей
            required_fields = ['id', 'date', 'message']
            for field in required_fields:
                if field not in message:
                    return False, f"Отсутствует обязательное поле: {field}"
            
            # Проверка ID
            if not isinstance(message['id'], int) or message['id'] <= 0:
                return False, "Неверный формат ID сообщения"
            
            # Проверка даты
            if self.validate_dates:
                try:
                    date_str = message['date']
                    if isinstance(date_str, str):
                        datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    return False, "Неверный формат даты"
            
            # Проверка текста
            text = message.get('message', '')
            if not isinstance(text, str):
                return False, "Текст сообщения должен быть строкой"
            
            if len(text) < self.min_text_length:
                return False, f"Текст слишком короткий (мин: {self.min_text_length})"
            
            if len(text) > self.max_text_length:
                return False, f"Текст слишком длинный (макс: {self.max_text_length})"
            
            # Проверка удаленных пользователей
            if self.remove_deleted_users:
                sender_info = message.get('sender_info', {})
                if sender_info.get('deleted', False):
                    return False, "Сообщение от удаленного пользователя"
            
            return True, None
            
        except Exception as e:
            return False, f"Ошибка валидации: {str(e)}"
    
    def clean_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Очищает и нормализует данные сообщения."""
        cleaned = message.copy()
        
        # Очистка текста
        if 'message' in cleaned and isinstance(cleaned['message'], str):
            text = cleaned['message']
            
            # Декодирование HTML entities
            if self.clean_html:
                text = html.unescape(text)
            
            # Удаление лишних пробелов
            text = re.sub(r'\s+', ' ', text).strip()
            
            # Удаление невидимых символов
            text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
            
            cleaned['message'] = text
        
        # Нормализация даты
        if 'date' in cleaned:
            date_str = cleaned['date']
            if isinstance(date_str, str):
                try:
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    cleaned['date'] = dt.isoformat()
                except:
                    pass
        
        # Очистка информации об отправителе
        if 'sender_info' in cleaned:
            sender_info = cleaned['sender_info']
            if isinstance(sender_info, dict):
                # Удаление пустых полей
                sender_info = {k: v for k, v in sender_info.items() if v is not None}
                cleaned['sender_info'] = sender_info
        
        # Очистка медиа информации
        for media_key in ['photo', 'video', 'audio', 'document', 'sticker']:
            if media_key in cleaned and not cleaned[media_key]:
                del cleaned[media_key]
        
        return cleaned
    
    def remove_duplicates_from_list(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Удаляет дубликаты сообщений из списка."""
        if not self.remove_duplicates:
            return messages
        
        seen_ids = set()
        unique_messages = []
        
        for message in messages:
            message_id = message.get('id')
            if message_id and message_id not in seen_ids:
                seen_ids.add(message_id)
                unique_messages.append(message)
            elif not message_id:
                # Сообщения без ID добавляем (например, системные)
                unique_messages.append(message)
        
        removed_count = len(messages) - len(unique_messages)
        if removed_count > 0:
            self.logger.info(f"Удалено дубликатов: {removed_count}")
        
        return unique_messages
    
    def validate_and_clean_batch(
        self,
        messages: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Валидирует и очищает пакет сообщений."""
        valid_messages = []
        invalid_messages = []
        
        for message in messages:
            is_valid, error = self.validate_message(message)
            
            if is_valid:
                cleaned = self.clean_message(message)
                valid_messages.append(cleaned)
            else:
                invalid_messages.append({
                    'message': message,
                    'error': error
                })
                self.logger.warning(f"Невалидное сообщение: {error}")
        
        # Удаление дубликатов
        valid_messages = self.remove_duplicates_from_list(valid_messages)
        
        return valid_messages, invalid_messages
    
    def get_validation_stats(
        self,
        original_count: int,
        valid_messages: List[Dict[str, Any]],
        invalid_messages: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Возвращает статистику валидации."""
        valid_count = len(valid_messages)
        invalid_count = len(invalid_messages)
        
        # Анализ причин отказа
        error_reasons = {}
        for invalid in invalid_messages:
            error = invalid['error']
            error_reasons[error] = error_reasons.get(error, 0) + 1
        
        return {
            'original_count': original_count,
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'valid_percentage': (valid_count / original_count * 100) if original_count > 0 else 0,
            'error_reasons': error_reasons,
            'removed_duplicates': original_count - valid_count - invalid_count
        }
    
    def export_validation_report(
        self,
        stats: Dict[str, Any],
        output_file: str = "validation_report.json"
    ) -> None:
        """Экспортирует отчет о валидации."""
        import json
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'validation_config': self.config,
            'statistics': stats,
            'error_breakdown': stats['error_reasons']
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Отчет о валидации сохранен: {output_file}")