
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import requests
from scripts.google_sh import get_api_in_master_table, save_in_gsh
from time import sleep
from scripts.date_export import get_date_range_for_export


def get_fullstats_advert(name, api):

    url_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    url2 = "https://advert-api.wildberries.ru/adv/v2/fullstats"

    start_date, end_date = get_date_range_for_export()

    start_date, end_date = start_date.strftime(
        '%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

    headers = {'Authorization': api}

    print(f'подключаюсь к таблице: {name}')
    promo_count = requests.get(url_count, headers=headers)

    if promo_count.status_code == 200:
        data = promo_count.json()

    else:
        print(
            f"❌ Ошибка запроса count: {promo_count.status_code}: {promo_count.text}")

    advertID = [adv['advertId'] for a in data['adverts']
                for adv in a['advert_list']]

    print(f"Длина advertID для кабинета {name} = {len(advertID)}")

    all_stats, chunk = [], 100
    for i in range(0, len(advertID), chunk):
        advertID_chank = advertID[i:i+chunk]

        params = [{'id': c, 'interval': {'begin': start_date, 'end': end_date}}
                  for c in advertID_chank]

        try:
            print(
                f'Начинаю fullstats запрос к {name} для получения статистикки рк.\nДата отчетного периода: {start_date}\nдата конца периода: {end_date}')

            response = requests.post(url2, headers=headers, json=params)

            if response .status_code == 400 and "no companies with correct intervals" in response .text:
                print(
                    f"\033[91m ⚠️ Нет данных по кампаниям в интервале ({start_date} — {end_date}) для {name}. Пропускаем.\033[0m")
                continue

            if response .status_code != 200:
                print(
                    f"\033[91m❌ Ошибка {response .status_code}: {response .text}\033[0m")
                continue

            full_stats = response .json()

            print(f"✅ Получено {len(full_stats)} записей")
            all_stats.extend(full_stats)

            print('спим 60 сек')
            sleep(60)

        except Exception as e:
            print(
                f"\033[91m🚨 Ошибка при запросе к fullstats для {name}: {e}\033[0m")

    top_level_campaign_metrics_df = pd.DataFrame([{
        'advertId': campaign.get('advertId'),
        'views': campaign.get('views', 0),
        'clicks': campaign.get('clicks', 0),
        'ctr': campaign.get('ctr', 0),
        'cpc': campaign.get('cpc', 0),
        'sum': campaign.get('sum', 0),
        'atbs': campaign.get('atbs', 0),
        'orders': campaign.get('orders', 0),
        'cr': campaign.get('cr', 0),
        'shks': campaign.get('shks', 0),
        'sum_price': campaign.get('sum_price', 0),

    } for campaign in all_stats])

    #  данные по артикулам до группировки
    camp_data = []
    for c in all_stats:
        for d in c['days']:
            for a in d['apps']:
                for nm in a['nm']:
                    nm['appType'] = a['appType']
                    nm['date'] = d['date']
                    nm['advertId'] = c['advertId']

                    camp_data.append(nm)

    camp_df = pd.DataFrame(camp_data)

    print(camp_df.columns.tolist(),
          f'Данные по статистике рк распакованы. Кабинет: {name}', sep='\n')

    camp_df['date'] = pd.to_datetime(camp_df['date']).dt.date

    camp_df = camp_df.rename(columns={
        'sum': 'SUM'
    })

    group_df = camp_df.groupby([
        'date', 'nmId', 'name'
    ]).agg({
        'views': 'sum',
        'clicks': 'sum',
        'atbs': 'sum',
        'orders': 'sum',
        'shks': 'sum',
        'sum_price': 'sum',
        'SUM': 'sum'

    }).reset_index()

    group_df['CTR'] = np.where(
        group_df['views'] == 0,
        0,
        (
            group_df['clicks'] / group_df['views'] * 100
        ).round(2)
    )

    group_df['CR'] = np.where(
        group_df['clicks'] == 0,
        0,
        (
            group_df['orders'] / group_df['clicks'] * 100
        ).round(2)
    )

    group_df['CPC'] = np.where(
        group_df['clicks'] == 0,
        0,
        (
            group_df['SUM'] / group_df['clicks']
        ).round(2)
    )

    group_df = group_df.drop(columns=[
        'advertId', 'appType'
    ], errors='ignore').sort_values(['SUM', 'views'],  ascending=[False, False]).reset_index(drop=True)

    group_df['nmId'] = group_df['nmId'].astype(int)
    # group_df['date'] = group_df['date'].astype(str)
    group_df['name'] = group_df['name'].replace(
        '', '-').fillna('-')

    for col in [cols for cols in group_df.columns if cols not in ['date', 'name']]:
        group_df[col] = group_df[col].fillna(0)

    group_df = group_df.rename(columns={
        'views': 'Просмотры',
        'clicks': 'Переходы',
        'SUM': 'Расходы,₽',
        'atbs': 'ДобавиливКорзину',
        'orders': 'КоличествоЗаказов',
        'shks': 'Кол-воЗаказанныхТоваров,шт',
        'sum_price': 'ЗаказовНаСумму,₽',
        'name': 'Предмет',
        'nmId': 'Артикул WB',
        'appType': 'ТипПлатформы',
        'date': 'Дата',
        'advertId': 'ID_Кампании'
    })

    print(f'Данные преобразованы! Кабинет: {name}')
    return group_df.sort_values(['Дата', 'Расходы,₽'], ascending=False)


if __name__ == '__main__':
    cabinet = {}

    for name, (api, _) in get_api_in_master_table().items():
        cabinet[name] = get_fullstats_advert(
            name=name, api=api)

    save_in_gsh(cabinet=cabinet, sheet_name='РК(api)')
