import pandas as pd
from datetime import datetime
from gspread_dataframe import set_with_dataframe
from scripts.gspread_client import get_gspread_client


def get_api_in_master_table(sheet_main='MASTERTABLE_План продаж', wsheet_name='Список таблиц') -> dict[str, str]:
    cabinets = {}
    try:
        # Подключаемся к таблице MASTERTABLE_План продаж к листу Списк таблиц
        print('🔐 Подключаюсь к gspread...')
        gs = get_gspread_client()

    except Exception as e:
        print(f"❌ Ошибка подключения к gspread:\n{e}")
        return cabinets
    try:
        print('📄 Открываю таблицу: MASTERTABLE_План продаж')

        master_sheet_name = gs.open(sheet_main)
        master_worksheet = master_sheet_name.worksheet(wsheet_name)
        result = master_worksheet.get_all_values()

        # Получаем данные, имя таблиц для дальнейшего подключения по кабинетам
        table_names = [row[1] for row in result[1:]]

        print(f"📋 Найдено {len(table_names)} таблиц")
    except Exception as e:
        print(f"❌ Ошибка при получении списка таблиц:\n{e}")
        return cabinets

    # проходим циклом по table_names, который содержит имена таблиц для подключения
    for table_name in table_names:
        print(f"\n🔄 Обрабатываю: {table_name}")
        try:

            spreadsheet = gs.open(table_name)
            title_sheet = spreadsheet.worksheet('Заглавный')

            # Подключаемся к ячейке B10 которая содержит дату начала отчетного периода
            print('Получаю дату')
            start_period = title_sheet.acell('B10').value.strip()

            if not start_period:
                raise ValueError(f"B10 пустая в таблице {table_name}")

            # форматируем дату, так как дата приходит в формате д.м.ггг
            print('Форматирую дату!')
            formatted_date = start_period.replace('.', '-')

            # конвертируем дату из формата д-м-гггг в гггг-м-д
            convert_date = datetime.strptime(
                formatted_date, "%d-%m-%Y").strftime("%Y-%m-%d")

            # подключаемся к ячейке B19 - там находится api_key
            api_key = title_sheet.acell('B9').value
            if not api_key:
                raise ValueError("Пустая ячейка B9")

            # удаляем ненужные символы, если они есть
            api_key = api_key.strip().replace(
                '\n', '').replace('\r', '').replace(' ', '')

            cabinets[table_name] = (api_key, convert_date)
            print(f"✅ Кабинет добавлен: {table_name} ({convert_date})")

        except Exception as e:
            print(f"❌ Ошибка при обработке {table_name}:\n{e}")
            continue
    print("\n🧾 Словарь сформирован.")
    return cabinets


def save_in_gsh(cabinet: dict[str, pd.DataFrame], sheet_name=None) -> None:

    gs = get_gspread_client()

    for name_table, df in cabinet.items():
        spreadsheet = gs.open(name_table)

        # остатки
        if sheet_name in ['Остатки (api)']:
            worksheet = spreadsheet.worksheet(sheet_name)
            worksheet.clear()

            set_with_dataframe(worksheet, df, row=1, col=1)
            print(
                f"✅ Данные успешно загружены в '{sheet_name}'!\nКабинет {name_table}")
            continue

        # Воронка продаж sales
        if sheet_name in ['Аналитика (api)']:
            worksheet = spreadsheet.worksheet(sheet_name)

            start_row = len(worksheet.get_all_values()) + 1

            set_with_dataframe(
                worksheet,
                df,
                row=start_row,
                col=1,
                include_column_header=False,
                resize=False
            )

            print(
                f"✅ Данные успешно загружены в '{sheet_name}'!\nКабинет {name_table}")
            continue

        # Рекламная кампания advert
        if sheet_name in ['РК(api)']:
            worksheet = spreadsheet.worksheet(sheet_name)

            existing_rows = len(worksheet.get_all_values())

            start_row = existing_rows + 1 if existing_rows > 0 else 1

            set_with_dataframe(
                worksheet,
                df,
                row=start_row,
                col=2,
                include_column_header=False,
                resize=False
            )
            print(
                f"✅ Данные успешно загружены в '{sheet_name}'!\nКабинет {name_table}")
            continue

        print(f"⚠️ Неизвестное имя листа: '{sheet_name}'")

    print("🏁 Все таблицы обработаны.")
