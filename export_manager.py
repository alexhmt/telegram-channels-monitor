"""
Модуль для экспорта данных в различные форматы.
Поддерживает JSON, CSV, Excel, HTML и другие форматы.
"""

import json
import csv
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
import aiofiles
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom

class ExportManager:
    """Управляет экспортом данных в различные форматы."""
    
    def __init__(self, output_dir: str = "downloads"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def _get_output_path(self, base_name: str, extension: str) -> Path:
        """Возвращает путь для выходного файла."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{base_name}_{timestamp}.{extension}"
        return self.output_dir / filename
    
    async def export_json(self, data: List[Dict[str, Any]], filename: str = "messages") -> str:
        """Экспорт в JSON формат."""
        output_path = self._get_output_path(filename, "json")
        
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(
                data,
                indent=2,
                ensure_ascii=False,
                default=self._json_serializer
            ))
        
        return str(output_path)
    
    async def export_jsonl(self, data: List[Dict[str, Any]], filename: str = "messages") -> str:
        """Экспорт в JSON Lines формат."""
        output_path = self._get_output_path(filename, "jsonl")
        
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            for item in data:
                json_line = json.dumps(item, ensure_ascii=False, default=self._json_serializer)
                await f.write(json_line + '\n')
        
        return str(output_path)
    
    async def export_csv(self, data: List[Dict[str, Any]], filename: str = "messages") -> str:
        """Экспорт в CSV формат."""
        if not data:
            raise ValueError("Нет данных для экспорта")
        
        output_path = self._get_output_path(filename, "csv")
        
        # Подготовка данных для CSV
        flat_data = []
        for item in data:
            flat_item = self._flatten_dict(item)
            flat_data.append(flat_item)
        
        # Получение всех возможных колонок
        all_keys = set()
        for item in flat_data:
            all_keys.update(item.keys())
        
        columns = sorted(all_keys)
        
        async with aiofiles.open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            await writer.writeheader()
            
            for item in flat_data:
                # Обработка None значений
                safe_item = {k: str(v) if v is not None else '' for k, v in item.items()}
                await writer.writerow(safe_item)
        
        return str(output_path)
    
    async def export_excel(self, data: List[Dict[str, Any]], filename: str = "messages") -> str:
        """Экспорт в Excel формат."""
        if not data:
            raise ValueError("Нет данных для экспорта")
        
        output_path = self._get_output_path(filename, "xlsx")
        
        # Подготовка данных
        flat_data = []
        for item in data:
            flat_item = self._flatten_dict(item)
            flat_data.append(flat_item)
        
        # Создание DataFrame
        df = pd.DataFrame(flat_data)
        
        # Экспорт в Excel
        df.to_excel(output_path, index=False, engine='openpyxl')
        
        return str(output_path)
    
    async def export_html(self, data: List[Dict[str, Any]], filename: str = "messages") -> str:
        """Экспорт в HTML формат."""
        output_path = self._get_output_path(filename, "html")
        
        html_content = self._generate_html(data)
        
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            await f.write(html_content)
        
        return str(output_path)
    
    async def export_xml(self, data: List[Dict[str, Any]], filename: str = "messages") -> str:
        """Экспорт в XML формат."""
        output_path = self._get_output_path(filename, "xml")
        
        root = ET.Element("messages")
        
        for item in data:
            message_elem = ET.SubElement(root, "message")
            self._dict_to_xml(message_elem, item)
        
        # Форматирование XML
        xml_str = ET.tostring(root, encoding='unicode')
        dom = minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ")
        
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            await f.write(pretty_xml)
        
        return str(output_path)
    
    async def export_markdown(self, data: List[Dict[str, Any]], filename: str = "messages") -> str:
        """Экспорт в Markdown формат."""
        output_path = self._get_output_path(filename, "md")
        
        md_content = self._generate_markdown(data)
        
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            await f.write(md_content)
        
        return str(output_path)
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Преобразует вложенный словарь в плоский."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                # Для списков словарей создаем отдельные колонки
                for i, item in enumerate(v):
                    items.extend(self._flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _json_serializer(self, obj):
        """Сериализатор для JSON."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)
    
    def _generate_html(self, data: List[Dict[str, Any]]) -> str:
        """Генерирует HTML контент."""
        html_parts = [
            '<!DOCTYPE html>',
            '<html lang="ru">',
            '<head>',
            '<meta charset="UTF-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1.0">',
            '<title>Telegram Messages Export</title>',
            '<style>',
            'body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }',
            '.message { border: 1px solid #ddd; margin: 10px 0; padding: 15px; border-radius: 5px; background-color: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }',
            '.message-header { font-weight: bold; margin-bottom: 10px; color: #2c3e50; }',
            '.message-content { margin: 10px 0; white-space: pre-wrap; }',
            '.message-date { color: #666; font-size: 0.9em; margin-bottom: 10px; }',
            '.message-media { margin-top: 15px; padding-top: 10px; border-top: 1px solid #eee; }',
            '.message-media img { border-radius: 5px; margin: 5px; cursor: pointer; transition: transform 0.2s; max-width: 300px; max-height: 300px; }',
            '.message-media img:hover { transform: scale(1.02); }',
            '.message-media video { border-radius: 5px; margin: 5px; max-width: 400px; max-height: 400px; }',
            '.message-media audio { margin: 5px; }',
            '.message-media a { color: #3498db; text-decoration: none; display: inline-block; margin: 5px; padding: 8px 12px; background-color: #f8f9fa; border-radius: 4px; border: 1px solid #dee2e6; }',
            '.message-media a:hover { text-decoration: underline; background-color: #e9ecef; }',
            '.media-error { color: #e74c3c; font-style: italic; }',
            '.modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.9); }',
            '.modal-content { margin: auto; display: block; max-width: 90%; max-height: 90%; margin-top: 40px; }',
            '.modal-content-file { margin: auto; display: block; width: 80%; height: 80%; margin-top: 40px; background-color: white; border-radius: 8px; padding: 20px; overflow: auto; }',
            '.close { position: absolute; top: 15px; right: 35px; color: #f1f1f1; font-size: 40px; font-weight: bold; cursor: pointer; }',
            '.close:hover, .close:focus { color: #bbb; text-decoration: none; }',
            '.modal-caption { margin: auto; display: block; width: 80%; max-width: 700px; text-align: center; color: #ccc; padding: 10px 0; }',
            '.modal-download { position: absolute; bottom: 20px; right: 20px; padding: 10px 15px; background-color: #4CAF50; color: white; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; }',
            '.modal-download:hover { background-color: #45a049; }',
            '</style>',
            '</head>',
            '<body>',
            '<h1>Экспорт сообщений Telegram</h1>',
            f'<p>Всего сообщений: {len(data)}</p>'
        ]
        
        for item in data:
            html_parts.append(self._generate_message_html(item))
        
        # Добавляем модальное окно для просмотра медиа
        html_parts.extend([
            '<!-- Модальное окно для просмотра медиа -->',
            '<div id="mediaModal" class="modal">',
            '  <span class="close">&times;</span>',
            '  <div id="modalContentContainer"></div>',
            '  <div id="modalCaption" class="modal-caption"></div>',
            '  <a id="modalDownload" class="modal-download" download>Скачать</a>',
            '</div>',
            
            '<script>',
            '// Получаем модальное окно',
            'var modal = document.getElementById("mediaModal");',
            '// Получаем элемент для закрытия модального окна',
            'var span = document.getElementsByClassName("close")[0];',
            '// Получаем контейнер для контента',
            'var modalContentContainer = document.getElementById("modalContentContainer");',
            '// Получаем элемент для подписи',
            'var modalCaption = document.getElementById("modalCaption");',
            '// Получаем элемент для скачивания',
            'var modalDownload = document.getElementById("modalDownload");',
            '',
            '// Функция для открытия изображения',
            'function openImage(src, caption, downloadUrl) {',
            '  modalContentContainer.innerHTML = \'<img class="modal-content" src="\' + src + \'">\';',
            '  modalCaption.textContent = caption;',
            '  modalDownload.href = downloadUrl;',
            '  modal.style.display = "block";',
            '}',
            '',
            '// Функция для открытия видео',
            'function openVideo(src, caption, downloadUrl) {',
            '  modalContentContainer.innerHTML = \'<video class="modal-content" controls autoplay><source src="\' + src + \'">Ваш браузер не поддерживает видео тег.</video>\';',
            '  modalCaption.textContent = caption;',
            '  modalDownload.href = downloadUrl;',
            '  modal.style.display = "block";',
            '}',
            '',
            '// Функция для открытия аудио',
            'function openAudio(src, caption, downloadUrl) {',
            '  modalContentContainer.innerHTML = \'<audio class="modal-content" controls autoplay><source src="\' + src + \'">Ваш браузер не поддерживает аудио тег.</audio>\';',
            '  modalCaption.textContent = caption;',
            '  modalDownload.href = downloadUrl;',
            '  modal.style.display = "block";',
            '}',
            '',
            '// Функция для открытия файла',
            'function openFile(src, caption, downloadUrl) {',
            '  modalContentContainer.innerHTML = \'<div class="modal-content-file"><iframe src="\' + src + \'" style="width: 100%; height: 100%; border: none;"></iframe></div>\';',
            '  modalCaption.textContent = caption;',
            '  modalDownload.href = downloadUrl;',
            '  modal.style.display = "block";',
            '}',
            '',
            '// Когда пользователь нажимает на (x), закрываем модальное окно',
            'span.onclick = function() {',
            '  modal.style.display = "none";',
            '  // Останавливаем видео/аудио при закрытии',
            '  var mediaElements = modalContentContainer.querySelectorAll("video, audio");',
            '  mediaElements.forEach(function(element) {',
            '    element.pause();',
            '    element.currentTime = 0;',
            '  });',
            '}',
            '',
            '// Когда пользователь нажимает в любом месте за пределами модального окна, закрываем его',
            'window.onclick = function(event) {',
            '  if (event.target == modal) {',
            '    modal.style.display = "none";',
            '    // Останавливаем видео/аудио при закрытии',
            '    var mediaElements = modalContentContainer.querySelectorAll("video, audio");',
            '    mediaElements.forEach(function(element) {',
            '      element.pause();',
            '      element.currentTime = 0;',
            '    });',
            '  }',
            '}',
            '</script>'
        ])
        
        html_parts.extend(['</body>', '</html>'])
        
        return '\n'.join(html_parts)
    
    def _generate_message_html(self, message: Dict[str, Any]) -> str:
        """Генерирует HTML для одного сообщения."""
        sender_info = message.get('sender_info', {})
        sender_name = sender_info.get('title') or f"{sender_info.get('first_name', '')} {sender_info.get('last_name', '')}".strip()
        
        date_str = message.get('date', '')
        text = message.get('message', '')
        
        html_parts = [
            '<div class="message">',
            f'<div class="message-header">{sender_name}</div>',
            f'<div class="message-date">{date_str}</div>',
            f'<div class="message-content">{text}</div>'
        ]
        
        # Добавление медиафайлов
        media_info = message.get('media_info')
        if media_info:
            html_parts.append('<div class="message-media">')
            
            if isinstance(media_info, dict):
                file_path = media_info.get('file_path')
                media_type = media_info.get('type', 'unknown')
                
                if file_path and Path(file_path).exists():
                    # Создаем относительный путь от HTML файла к медиафайлу
                    relative_path = self._get_relative_media_path(file_path)
                    
                    if media_type == 'photo':
                        # Добавляем возможность открытия изображения во всплывающем окне
                        filename = Path(file_path).name
                        html_parts.append(f'<img src="{relative_path}" alt="Photo" onclick="openImage(\'{relative_path}\', \'{filename}\', \'{relative_path}\')" title="Нажмите для увеличения">')
                    elif media_type == 'video':
                        # Добавляем возможность открытия видео во всплывающем окне
                        filename = Path(file_path).name
                        html_parts.append(f'<video controls onclick="openVideo(\'{relative_path}\', \'{filename}\', \'{relative_path}\')" title="Нажмите для увеличения"><source src="{relative_path}" type="video/mp4">Your browser does not support the video tag.</video>')
                    elif media_type == 'audio':
                        # Добавляем возможность открытия аудио во всплывающем окне
                        filename = Path(file_path).name
                        html_parts.append(f'<audio controls onclick="openAudio(\'{relative_path}\', \'{filename}\', \'{relative_path}\')" title="Нажмите для открытия в плеере"><source src="{relative_path}" type="audio/mpeg">Your browser does not support the audio tag.</audio>')
                    elif media_type == 'document':
                        # Добавляем возможность открытия документа во всплывающем окне
                        filename = Path(file_path).name
                        html_parts.append(f'<a href="#" onclick="openFile(\'{relative_path}\', \'{filename}\', \'{relative_path}\'); return false;">📄 Открыть документ: {filename}</a>')
                    elif media_type == 'voice':
                        # Добавляем возможность открытия голосового сообщения во всплывающем окне
                        filename = Path(file_path).name
                        html_parts.append(f'<audio controls onclick="openAudio(\'{relative_path}\', \'Голосовое сообщение: {filename}\', \'{relative_path}\')" title="Нажмите для открытия в плеере"><source src="{relative_path}" type="audio/ogg">Your browser does not support the audio tag.</audio>')
                    else:
                        # Для других типов файлов
                        filename = Path(file_path).name
                        html_parts.append(f'<a href="#" onclick="openFile(\'{relative_path}\', \'{filename}\', \'{relative_path}\'); return false;">📎 Открыть файл: {filename}</a>')
                else:
                    html_parts.append(f'<div class="media-error">Media file not found: {file_path}</div>')
            elif isinstance(media_info, list):
                # Несколько медиафайлов
                for media in media_info:
                    file_path = media.get('file_path')
                    media_type = media.get('type', 'unknown')
                    
                    if file_path and Path(file_path).exists():
                        relative_path = self._get_relative_media_path(file_path)
                        
                        if media_type == 'photo':
                            filename = Path(file_path).name
                            html_parts.append(f'<img src="{relative_path}" alt="Photo" onclick="openImage(\'{relative_path}\', \'{filename}\', \'{relative_path}\')" title="Нажмите для увеличения" style="max-width: 300px; max-height: 300px; margin: 5px;">')
                        elif media_type == 'video':
                            filename = Path(file_path).name
                            html_parts.append(f'<video controls onclick="openVideo(\'{relative_path}\', \'{filename}\', \'{relative_path}\')" title="Нажмите для увеличения" style="max-width: 400px; max-height: 400px; margin: 5px;"><source src="{relative_path}" type="video/mp4">Your browser does not support the video tag.</video>')
                        elif media_type == 'audio':
                            filename = Path(file_path).name
                            html_parts.append(f'<audio controls onclick="openAudio(\'{relative_path}\', \'{filename}\', \'{relative_path}\')" title="Нажмите для открытия в плеере" style="margin: 5px;"><source src="{relative_path}" type="audio/mpeg">Your browser does not support the audio tag.</audio>')
                        elif media_type == 'document':
                            filename = Path(file_path).name
                            html_parts.append(f'<a href="#" onclick="openFile(\'{relative_path}\', \'{filename}\', \'{relative_path}\'); return false;" style="margin: 5px;">📄 Открыть документ: {filename}</a>')
                        elif media_type == 'voice':
                            filename = Path(file_path).name
                            html_parts.append(f'<audio controls onclick="openAudio(\'{relative_path}\', \'Голосовое сообщение: {filename}\', \'{relative_path}\')" title="Нажмите для открытия в плеере" style="margin: 5px;"><source src="{relative_path}" type="audio/ogg">Your browser does not support the audio tag.</audio>')
                        else:
                            filename = Path(file_path).name
                            html_parts.append(f'<a href="#" onclick="openFile(\'{relative_path}\', \'{filename}\', \'{relative_path}\'); return false;" style="margin: 5px;">📎 Открыть файл: {filename}</a>')
                    else:
                        html_parts.append(f'<div class="media-error">Media file not found: {file_path}</div>')
            
            html_parts.append('</div>')
        
        html_parts.append('</div>')
        
        return '\n'.join(html_parts)
    
    def _get_relative_media_path(self, file_path: str) -> str:
        """Создает относительный путь от HTML файла к медиафайлу."""
        file_path = Path(file_path)
        
        # Если файл находится в downloads/media/*, создаем относительный путь
        if 'downloads' in file_path.parts and 'media' in file_path.parts:
            # Находим индекс 'media' в пути
            parts = file_path.parts
            try:
                media_index = parts.index('media')
                # Берем путь от 'media' включительно
                relative_parts = parts[media_index:]
                return '/'.join(relative_parts)
            except ValueError:
                # Если 'media' не найдено, используем только имя файла
                return file_path.name
        else:
            # Для других случаев используем только имя файла
            return file_path.name
    
    def _generate_markdown(self, data: List[Dict[str, Any]]) -> str:
        """Генерирует Markdown контент."""
        md_parts = [
            "# Экспорт сообщений Telegram",
            "",
            f"**Всего сообщений:** {len(data)}",
            f"**Дата экспорта:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "---",
            ""
        ]
        
        for item in data:
            md_parts.append(self._generate_message_markdown(item))
            md_parts.append("")
        
        return '\n'.join(md_parts)
    
    def _generate_message_markdown(self, message: Dict[str, Any]) -> str:
        """Генерирует Markdown для одного сообщения."""
        sender_info = message.get('sender_info', {})
        sender_name = sender_info.get('title') or f"{sender_info.get('first_name', '')} {sender_info.get('last_name', '')}".strip()
        
        date_str = message.get('date', '')
        text = message.get('message', '')
        message_id = message.get('id', '')
        
        # Получение ID сообщения, на которое ссылается ответ
        reply_to_id = ""
        reply_to = message.get('reply_to', {})
        if reply_to:
            reply_to_id = reply_to.get('reply_to_msg_id', '')
        
        # Формирование строки с ID сообщения и reply_to
        if reply_to_id:
            id_line = f"`ID: {message_id}`; reply_to: {reply_to_id}"
        else:
            id_line = f"`ID: {message_id}`"
        
        md_parts = [
            f"**{sender_name}**",
            f"*{date_str}*",
            id_line,
            ""
        ]
        
        # Добавление цитаты (reply)
        if reply_to:
            reply_msg = reply_to.get('message', {})
            if reply_msg:
                reply_sender = reply_msg.get('sender_info', {})
                reply_sender_name = reply_sender.get('title') or f"{reply_sender.get('first_name', '')} {reply_sender.get('last_name', '')}".strip()
                reply_text = reply_msg.get('message', '')
                md_parts.append(f"> **{reply_sender_name}:** {reply_text}")
                md_parts.append("")
        
        # Добавление пересылки (forward)
        forward = message.get('fwd_from', {})
        if forward:
            forward_sender = forward.get('from_id', {})
            forward_sender_name = "Неизвестно"
            if forward_sender:
                if forward_sender.get('_'):
                    if forward_sender['_'] == 'PeerUser':
                        user_id = forward_sender.get('user_id')
                        # В реальной реализации нужно получить имя пользователя по ID
                        forward_sender_name = f"Пользователь {user_id}"
                    elif forward_sender['_'] == 'PeerChat':
                        chat_id = forward_sender.get('chat_id')
                        forward_sender_name = f"Чат {chat_id}"
                    elif forward_sender['_'] == 'PeerChannel':
                        channel_id = forward_sender.get('channel_id')
                        forward_sender_name = f"Канал {channel_id}"
            
            forward_date = forward.get('date', '')
            if forward_date:
                forward_date_str = forward_date.strftime('%Y-%m-%d %H:%M:%S')
                md_parts.append(f"*Переслано от {forward_sender_name} {forward_date_str}*")
                md_parts.append("")
        
        # Добавление текста сообщения
        if text:
            md_parts.append(text)
            md_parts.append("")
        
        # Добавление медиафайлов
        media_info = message.get('media_info')
        if media_info:
            md_parts.append("**Медиафайлы:**")
            
            if isinstance(media_info, dict):
                file_path = media_info.get('file_path')
                media_type = media_info.get('type', 'unknown')
                file_name = Path(file_path).name if file_path else "Неизвестно"
                
                if file_path and Path(file_path).exists():
                    relative_path = self._get_relative_media_path(file_path)
                    
                    if media_type == 'photo':
                        md_parts.append(f"![Фото]({relative_path})")
                    elif media_type == 'video':
                        md_parts.append(f"[Видео]({relative_path})")
                    elif media_type == 'audio':
                        md_parts.append(f"[Аудио]({relative_path})")
                    elif media_type == 'document':
                        md_parts.append(f"[Документ]({relative_path})")
                    elif media_type == 'voice':
                        md_parts.append(f"[Голосовое сообщение]({relative_path})")
                    else:
                        md_parts.append(f"[Файл]({relative_path})")
                else:
                    md_parts.append(f"Файл не найден: {file_name}")
            elif isinstance(media_info, list):
                for media in media_info:
                    file_path = media.get('file_path')
                    media_type = media.get('type', 'unknown')
                    file_name = Path(file_path).name if file_path else "Неизвестно"
                    
                    if file_path and Path(file_path).exists():
                        relative_path = self._get_relative_media_path(file_path)
                        
                        if media_type == 'photo':
                            md_parts.append(f"![Фото]({relative_path})")
                        elif media_type == 'video':
                            md_parts.append(f"[Видео]({relative_path})")
                        elif media_type == 'audio':
                            md_parts.append(f"[Аудио]({relative_path})")
                        elif media_type == 'document':
                            md_parts.append(f"[Документ]({relative_path})")
                        elif media_type == 'voice':
                            md_parts.append(f"[Голосовое сообщение]({relative_path})")
                        else:
                            md_parts.append(f"[Файл]({relative_path})")
                    else:
                        md_parts.append(f"Файл не найден: {file_name}")
            
            md_parts.append("")
        
        return '\n'.join(md_parts)
    
    def _dict_to_xml(self, parent: ET.Element, data: Dict[str, Any]) -> None:
        """Преобразует словарь в XML элементы."""
        for key, value in data.items():
            if isinstance(value, dict):
                child = ET.SubElement(parent, key)
                self._dict_to_xml(child, value)
            elif isinstance(value, list):
                for item in value:
                    child = ET.SubElement(parent, key)
                    if isinstance(item, dict):
                        self._dict_to_xml(child, item)
                    else:
                        child.text = str(item)
            else:
                child = ET.SubElement(parent, key)
                child.text = str(value) if value is not None else ""
    
    async def export_multiple_formats(
        self,
        data: List[Dict[str, Any]],
        formats: List[str],
        base_filename: str = "messages"
    ) -> Dict[str, str]:
        """Экспортирует данные в несколько форматов."""
        results = {}
        
        format_handlers = {
            'json': self.export_json,
            'jsonl': self.export_jsonl,
            'csv': self.export_csv,
            'excel': self.export_excel,
            'html': self.export_html,
            'xml': self.export_xml,
            'markdown': self.export_markdown,
            'md': self.export_markdown
        }
        
        for fmt in formats:
            if fmt in format_handlers:
                try:
                    filepath = await format_handlers[fmt](data, base_filename)
                    results[fmt] = filepath
                except Exception as e:
                    results[fmt] = f"Ошибка: {str(e)}"
        
        return results