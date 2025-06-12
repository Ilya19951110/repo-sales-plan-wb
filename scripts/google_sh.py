import gspread
from datetime import datetime
from gspread_dataframe import set_with_dataframe


def get_api_key():

    gs = gspread.service_account(
        filename=r'C:\Users\Ilya\OneDrive\scripts\myanaliticmp-0617169ebf44.json'
    )

    spreadsheet = gs.open('План продаж ИП Мартыненко')

    title_sheet = spreadsheet.worksheet('Заглавный')
    start_period = title_sheet.acell('B10').value.strip()

    try:
        formatted_date = start_period.replace('.', '-')
        convert_date = datetime.strptime(
            formatted_date, "%d-%m-%Y").strftime("%Y-%m-%d")

        print("🗓 Конвертировано:", convert_date)
    except Exception as e:
        print("❌ Ошибка при обработке даты:", start_period, f"{e}", sep='\n')

    api_key = title_sheet.acell('B9').value.strip().replace(
        '\n', '').replace('\r', '').replace(' ', '')

    return api_key, convert_date


def save_in_gsh(df, sheet_name=None):
    gs = gspread.service_account(
        filename=r'C:\Users\Ilya\OneDrive\scripts\myanaliticmp-0617169ebf44.json')

    spreadsheet = gs.open('План продаж ИП Мартыненко')

    # остатки
    if sheet_name in ['Остатки (api)']:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()

        set_with_dataframe(worksheet, df, row=1, col=1)
        print(f"✅ Данные успешно загружены в '{sheet_name}'!")
        return

    # Воронка продаж salles
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

        print(f"✅ Данные успешно загружены в '{sheet_name}'!")
        return

    # Рекламная кампания advert
    if sheet_name in ['РК(api)']:
        worksheet = spreadsheet.worksheet(sheet_name)

        sheet_rows = worksheet.row_count
        sheet_cols = worksheet.col_count

        if sheet_cols > 1:
            clear_range = f"B1:{gspread.utils.rowcol_to_a1(sheet_rows, sheet_cols)}"
            worksheet.batch_clear([clear_range])
            print(f"🧹 Удалён диапазон: {clear_range}")
        else:
            print("ℹ️ В листе только столбец A — ничего не удалено.")

        set_with_dataframe(worksheet, df, row=1, col=2)
        print(f"✅ Данные успешно загружены в '{sheet_name}'!")
        return

    print(f"⚠️ Неизвестное имя листа: '{sheet_name}'")
