import os
import gspread


def get_gspread_client():
    """
    Универсальное подключение к Google Sheets API:
    - Локально: через GSPREAD_JSON или key.json
    - GitHub Actions: через key.json, созданный из секрета
    """

    env_path = os.environ.get("GSPREAD_JSON")
    default_path = 'key.json'

    if env_path:
        clean_path = env_path.strip('"').replace("\\", '/')

        if os.path.exists(clean_path):
            print(f"✅ Используем JSON-файл: 'GSPREAD_JSON'")
            return gspread.service_account(filename=clean_path)

    if os.path.exists(default_path):
        print(f"✅ Используем JSON-файл: 'key.json'")
        return gspread.service_account(filename=default_path)

    raise FileExistsError(
        f"⚠️ Не найден service account JSON-файл\nУстановите переменную окружения GSPREAD_JSON или создайте key.json.")
