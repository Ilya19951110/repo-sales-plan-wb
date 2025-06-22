import requests
import pandas as pd
from scripts.google_sh import save_in_gsh, get_api_in_master_table


def get_supplier_stocks(api, date, name):
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

    headers = {"Authorization": api}

    params = {
        "dateFrom": date
    }

    data_stocks = pd.DataFrame()
    try:
        print(f"🚀🚀  Начинаю запрос ОСТАТКОВ к {name} ")

        res = requests.get(url, params=params, headers=headers)

        if res.status_code != 200:
            raise ValueError(f"⚠️ ⚠️   Ошибка запроса 'ОСТАТКОВ' {res.text()}")

        stocks = res.json()

        if not stocks:
            print(f"⚠️ Пустой ответ от API, кабинет {name}\n{url}")

        data_stocks = pd.DataFrame(stocks)

        data_stocks = data_stocks.rename(columns={
            'lastChangeDate': 'ДатаОбновления',
            'warehouseName': 'НазваниеСклада',
            'nmId': 'Артикул WB',
            'barcode': 'баркод',
            'quantity': 'Остатки',
            'inWayToClient': 'ВпутиКлиенту',
            'inWayFromClient': 'ВпутиОтКлиента',
            'quantityFull': 'ВсегоОстатков',
            'category': 'Категория',
            'subject': 'Предмет',
            'brand': 'Бренд',
            'techSize': 'Размер',
            'Price': 'Цена',
            'Discount': 'Скидка',
            'isSupply': 'ДоговорПоставки',
            'isRealization': 'договоРеализации',
            'SCCode': 'КодКонтракта',
            'supplierArticle': 'АртикулПродавца'
        })
        data_stocks['ДатаОбновления'] = pd.to_datetime(
            data_stocks['ДатаОбновления']).dt.date

        data_stocks['Размер'] = data_stocks['Размер'].astype(str)

    except Exception as e:
        print(f"❌❌  Произошла ошибка кабинета {name} при вызове: {e}")
    return data_stocks


if __name__ == '__main__':
    cabinet = {}

    for name, (api, date) in get_api_in_master_table().items():
        cabinet[name] = get_supplier_stocks(name=name, api=api, date=date)

    # promo_count(api=api)
    save_in_gsh(cabinet=cabinet, sheet_name='Остатки (api)')
