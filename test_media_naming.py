#!/usr/bin/env python3
"""
Тестовый скрипт для проверки исправлений в именовании медиафайлов и HTML экспорте.
"""

import sys
from pathlib import Path
from media_downloader import MediaDownloader
from export_manager import ExportManager

def test_media_filename_generation():
    """Тестирует генерацию имен файлов на основе ID сообщения."""
    print("=== Тест генерации имен медиафайлов ===")
    
    downloader = MediaDownloader("downloads/media")
    
    # Тестовые случаи
    test_cases = [
        {
            'media_type': 'photo',
            'original_filename': 'IMG_20250720_123456.jpg',
            'extension': None,
            'message_id': 160883,
            'expected': '160883.jpg'
        },
        {
            'media_type': 'video',
            'original_filename': 'video.mp4',
            'extension': None,
            'message_id': 160890,
            'expected': '160890.mp4'
        },
        {
            'media_type': 'document',
            'original_filename': 'document.pdf',
            'extension': None,
            'message_id': 160902,
            'expected': '160902.pdf'
        },
        {
            'media_type': 'audio',
            'original_filename': None,
            'extension': '.mp3',
            'message_id': 160903,
            'expected': '160903.mp3'
        }
    ]
    
    all_passed = True
    for i, case in enumerate(test_cases, 1):
        result = downloader._generate_filename(
            case['media_type'],
            case['original_filename'],
            case['extension'],
            case['message_id']
        )
        
        passed = result == case['expected']
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"Тест {i}: {status}")
        print(f"  Ожидалось: {case['expected']}")
        print(f"  Получено:  {result}")
        
        if not passed:
            all_passed = False
    
    return all_passed

def test_html_media_paths():
    """Тестирует генерацию относительных путей к медиафайлам в HTML."""
    print("\n=== Тест HTML путей к медиафайлам ===")
    
    export_manager = ExportManager("downloads")
    
    # Тестовые случаи
    test_cases = [
        {
            'file_path': 'downloads/media/photos/160883.jpg',
            'expected': 'media/photos/160883.jpg'
        },
        {
            'file_path': 'downloads/media/videos/160890.mp4',
            'expected': 'media/videos/160890.mp4'
        },
        {
            'file_path': 'downloads/media/documents/160902.pdf',
            'expected': 'media/documents/160902.pdf'
        },
        {
            'file_path': '/absolute/path/to/file.txt',
            'expected': 'file.txt'
        }
    ]
    
    all_passed = True
    for i, case in enumerate(test_cases, 1):
        result = export_manager._get_relative_media_path(case['file_path'])
        
        passed = result == case['expected']
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"Тест {i}: {status}")
        print(f"  Путь:      {case['file_path']}")
        print(f"  Ожидалось: {case['expected']}")
        print(f"  Получено:  {result}")
        
        if not passed:
            all_passed = False
    
    return all_passed

def test_html_generation():
    """Тестирует генерацию HTML с медиафайлами."""
    print("\n=== Тест генерации HTML ===")
    
    export_manager = ExportManager("downloads")
    
    # Тестовое сообщение с медиафайлом
    test_message = {
        'id': 160883,
        'date': '2025-07-20 12:00:00+00:00',
        'message': 'Тестовое сообщение с фото',
        'sender_info': {
            'first_name': 'Тест',
            'last_name': 'Пользователь'
        },
        'media_info': {
            'type': 'photo',
            'file_path': 'downloads/media/photos/160883.jpg'
        }
    }
    
    html = export_manager._generate_message_html(test_message)
    
    # Проверяем, что HTML содержит правильный путь к медиафайлу
    expected_path = 'media/photos/160883.jpg'
    contains_correct_path = expected_path in html
    
    status = "✓ PASS" if contains_correct_path else "✗ FAIL"
    print(f"Тест генерации HTML: {status}")
    print(f"  Ожидаемый путь: {expected_path}")
    print(f"  Найден в HTML: {contains_correct_path}")
    
    if contains_correct_path:
        print("  HTML содержит корректный относительный путь к медиафайлу")
    else:
        print("  HTML НЕ содержит корректный путь к медиафайлу")
        print(f"  Сгенерированный HTML:\n{html}")
    
    return contains_correct_path

def main():
    """Запускает все тесты."""
    print("Запуск тестов для проверки исправлений...")
    
    test1_passed = test_media_filename_generation()
    test2_passed = test_html_media_paths()
    test3_passed = test_html_generation()
    
    print("\n" + "="*50)
    print("РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print(f"Генерация имен файлов: {'✓ PASS' if test1_passed else '✗ FAIL'}")
    print(f"HTML пути к медиафайлам: {'✓ PASS' if test2_passed else '✗ FAIL'}")
    print(f"Генерация HTML: {'✓ PASS' if test3_passed else '✗ FAIL'}")
    
    all_passed = test1_passed and test2_passed and test3_passed
    
    if all_passed:
        print("\n🎉 ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        print("\nИсправления работают корректно:")
        print("- Медиафайлы именуются только по ID сообщения")
        print("- HTML файлы и медиафайлы сохраняются в папку downloads")
        print("- HTML содержит корректные относительные ссылки на медиафайлы")
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ")
        print("Необходимо проверить и исправить код")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())