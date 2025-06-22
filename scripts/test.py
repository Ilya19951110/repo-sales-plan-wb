
import requests
import json
import pandas as pd
from datetime import date


def func():
    url_count = 'https://advert-api.wildberries.ru/adv/v1/promotion/count'
    headers = {"Authorization": 'eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwNTIwdjEiLCJ0eXAiOiJKV1QifQ.eyJlbnQiOjEsImV4cCI6MTc2NTUxMjE3OCwiaWQiOiIwMTk3NjRlMS01NTljLTcxZTItYThjOS01NmYxMDk0YmY1ZDkiLCJpaWQiOjIyODQ2NTQ2LCJvaWQiOjU2OTQzOSwicyI6MTYxMjYsInNpZCI6IjVhMmY2ZjJkLTg5ZjctNDI4Zi05MDMyLTU5Njk2OGZmMTdlOCIsInQiOmZhbHNlLCJ1aWQiOjIyODQ2NTQ2fQ.OBucSzJyD4LnnVnUUQJGCF0ayxvrkjzCkQjFZ_PG1ntS4b_oDjkcfAzlHb8DrW0Mb1kDURjsFddyZDVsoILqKg'}

    result = requests.get(url_count, headers=headers)
    result_json = result.json()
    save_path = r'C:\Users\Ilya\OneDrive\repo_sales_plan\scripts\save.csv'
    with open(save_path, 'w', encoding='utf-8') as file:
        json.dump(result_json, file, ensure_ascii=False, indent=2)
    print('данные сохранены')


def read_json_data():
    save_path = r'C:\Users\Ilya\OneDrive\repo_sales_plan\scripts\save.csv'
    with open(save_path, 'r', encoding='utf-8') as writter:
        data = json.load(writter)

    # for adv in data['adverts']:
    #     for a in adv['advert_list']:
    #         print(a)

    advert = pd.DataFrame([a for adv in data['adverts']
                          for a in adv['advert_list']])

    advert['changeTime'] = pd.to_datetime(advert['changeTime']).dt.date

    filter_advert = advert[
        (advert['changeTime'] >= date(2025, 6, 16))
        &
        (advert['changeTime'] <= date(2025, 6, 18))
    ]
    advert = advert.sort_values(['changeTime'], ascending=False)
    print(advert.head(30).reset_index(drop=True))


read_json_data()
