
import pandas as pd
from datetime import datetime, timedelta
import requests
from scripts.google_sh import get_api_in_master_table, save_in_gsh
from time import sleep
import time
import tracemalloc
from scripts.date_export import get_date_range_for_export


def get_report_detail_sales(api, name):
    url = 'https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail'
    all_data = []
    stop = 21
    page = 1

    begin_date, end_date = get_date_range_for_export()
    begin_date = begin_date.strftime("%Y-%m-%d 00:00:00")
    end_date = end_date.strftime("%Y-%m-%d 23:59:59")

    while True:

        headers = {
            'Authorization': api,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        params = {

            'timezone': 'Europe/Moscow',
            'period': {

                'begin': begin_date,
                'end': end_date
            },
            'orderBy': {
                'field': 'openCard',
                'mode': 'desc'
            },
            'page': page
        }
        print(f'Подключаюсь к кабинету: {name}')
        requests_report = requests.post(url, headers=headers, json=params)

        if requests_report.status_code != 200:
            print(f"❌ Ошибка API: {requests_report.status_code}")
            print(requests_report.text)
            break

        result = requests_report.json()

        cards = result.get('data', {}).get('cards', [])

        if not cards:
            break

        all_data.extend(cards)

        print(
            f"\033[92mПолучено {len(cards)} записей кабинета. Всего: {len(all_data)}\033[0m")

        if len(cards) < 1_000:
            break
        page += 1
        print(f"⏱ Спим {stop} сек...")

        sleep(stop)

    return all_data


def get_current_week_sales_df(sales):

    def read_to_json(data, parent_key='', sep='_'):
        items = []

        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(read_to_json(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))

        return dict(items)

    data = [read_to_json(item) for item in sales]
    df = pd.DataFrame(data)

    assert isinstance(df, pd.DataFrame), "Входной df должен быть DataFrame"

    columns_rus = [
        'Артикул WB', 'Артикул поставщика', 'Бренд', 'ID категории', 'Название категории', 'Начало текущего периода', 'Конец текущего периода', 'Просмотры карточки', 'Добавления в корзину',
        'Количество заказов', 'Сумма заказов (руб)', 'Количество выкупов', 'Сумма выкупов (руб)', 'Количество отмен', 'Сумма отмен (руб)', 'Среднее заказов в день', 'Средняя цена (руб)',
        'Конверсия в корзину (%)', 'Конверсия в заказ (%)', 'Конверсия в выкуп (%)', 'Начало предыдущего периода', 'Конец предыдущего периода', 'Просмотры карточки (пред.)', 'Добавления в корзину (пред.)',
        'Количество заказов (пред.)', 'Сумма заказов (пред., руб)', 'Количество выкупов (пред.)', 'Сумма выкупов (пред., руб)', 'Количество отмен (пред.)', 'Сумма отмен (пред., руб)', 'Среднее заказов в день (пред.)',
        'Средняя цена (пред., руб)', 'Конверсия в корзину (пред., %)', 'Конверсия в заказ (пред., %)', 'Конверсия в выкуп (пред., %)', 'Динамика просмотров (%)', 'Динамика корзины (%)',
        'Динамика заказов (%)', 'Динамика суммы заказов (%)', 'Динамика выкупов (%)', 'Динамика суммы выкупов (%)', 'Динамика отмен (%)', 'Динамика суммы отмен (%)', 'Динамика ср. заказов в день (%)',
        'Динамика ср. цены (%)', 'Изменение конверсии в корзину', 'Изменение конверсии в заказ', 'Изменение конверсии в выкуп', 'Остатки на маркетплейсе', 'Остатки на WB'
    ]

    df = df.rename(
        columns=dict(zip(df.columns.tolist(), columns_rus))
    )

    date_col = ['Начало текущего периода', 'Конец текущего периода',
                'Начало предыдущего периода', 'Конец предыдущего периода']

    df[date_col] = df[date_col].apply(
        pd.to_datetime, errors='coerce').apply(lambda x: x.dt.date)

    current_week_col = df.columns[:df.columns.get_loc(
        'Начало предыдущего периода')].tolist() + df.columns[-3:].tolist()

    current_week = df[current_week_col].copy()

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода'])

    current_week['Номнед'] = current_week['Начало текущего периода'].dt.isocalendar().week

    current_week['Начало текущего периода'] = pd.to_datetime(
        current_week['Начало текущего периода']).dt.date

    current_week = current_week.drop_duplicates()
    print(current_week.columns.tolist(), '', current_week.head(5), sep='\n')

    return current_week


if __name__ == '__main__':
    tracemalloc.start()
    begin = time.time()
    cabinet = {}

    for name, (api, _) in get_api_in_master_table().items():
        try:
            print(f"🔄 Обработка: {name}")
            data_json = get_report_detail_sales(api=api, name=name)
            cabinet[name] = get_current_week_sales_df(data_json)

        except Exception as e:
            print(f"❌ Ошибка при обработке {name}: {e}")

    save_in_gsh(cabinet=cabinet, sheet_name='Аналитика (api)')
    end = time.time()

    print(f"Время выполнения get_sales: {(end-begin)/60:,.2f}")

    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')

    current, peak = tracemalloc.get_traced_memory()
    print(
        f"🔍 Память сейчас: {current / 10**6:.2f} MB; Пик: {peak / 10**6:.2f} MB")

    print("[ТОП по памяти:]")
    for stat in top_stats[:5]:
        print(stat)
