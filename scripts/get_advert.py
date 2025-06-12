
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
from scripts.google_sh import get_api_key, save_in_gsh
from time import sleep


def split_period(start_date: str, end_date: str, chunk_days: int = 31):
    ranges = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while start <= end:
        chunk_end = min(start + timedelta(days=chunk_days - 4), end)

        ranges.append((start.strftime("%Y-%m-%d"),
                      chunk_end.strftime("%Y-%m-%d")))

        start = chunk_end + timedelta(days=1)
    return ranges


def get_fullstats_advert(api, date):

    url_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    url2 = "https://advert-api.wildberries.ru/adv/v2/fullstats"
    sp = []

    date_from = date
    date_to = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    headers = {'Authorization': api}

    promo_count = requests.get(url_count, headers=headers)

    if promo_count.status_code == 200:
        data = promo_count.json()

    for block in data['adverts']:
        advert_type = block['type']
        advert_count = block['count']
        advert_status = block['status']

        for advert in block['advert_list']:
            advert['type'] = advert_type
            advert['status'] = advert_status
            advert['count'] = advert_count
            sp.append(advert)

    df = pd.DataFrame(sp)

    df['changeTime'] = pd.to_datetime(df['changeTime']).dt.date

    df = df[df['changeTime'] >= datetime.strptime(
        date, '%Y-%m-%d').date()]

    date_ranges = split_period(date, date_to)

    for i, (start, end) in enumerate(split_period(date_from, date_to)):
        print(f"{i}. {start} - {end}")

    all_stats = []
    for i, (start, end) in enumerate(date_ranges, 1):

        params = [{'id': c, 'interval': {'begin': start, 'end': end}}
                  for c in df['advertId']]

        try:
            requests_stats = requests.post(url2, headers=headers, json=params)

            if requests_stats.status_code == 200:
                full_stats = requests_stats.json()
                print(f"✅ Получено {len(full_stats)} записей")
                all_stats.extend(full_stats)

                # print('спим 60 сек')
                # sleep(60)
        except Exception as e:
            print(
                f'Ошибка запрос:\nСтатус: {requests_stats.status_code}\nОшибка: {e},\nТекст: {requests_stats.text}')

    save = r'C:\Users\Ilya\OneDrive\scripts\save.txt'
    with open(save, 'w', encoding='utf-8') as file:
        json.dump(all_stats, file, ensure_ascii=False, indent=2)
    # агрегированные данные по id рк

    first_level = [{
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

    } for campaign in all_stats]

    df_first = pd.DataFrame(first_level)

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
    print(camp_df.columns.tolist())
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

    return group_df.sort_values(['Дата', 'Расходы,₽'], ascending=False)


if __name__ == '__main__':
    api, date = get_api_key()

    # promo_count(api=api)
    df = get_fullstats_advert(api=api, date=date)

    save_in_gsh(df=df, sheet_name='РК(api)')
