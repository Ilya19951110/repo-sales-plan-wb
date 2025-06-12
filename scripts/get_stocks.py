import requests
import pandas as pd
from scripts.google_sh import get_api_key, save_in_gsh


def get_supplier__stocks(api, date):
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

    headers = {"Authorization": api}

    params = {
        "dateFrom": date
    }

    data_stocks = pd.DataFrame()
    try:
        print(f"🚀🚀  Начинаю запрос ОСТАТКОВ ")

        res = requests.get(url, params=params, headers=headers)

        if res.status_code != 200:
            raise ValueError(f"⚠️ ⚠️   Ошибка запроса 'ОСТАТКОВ' {res.text()}")

        stocks = res.json()

        if not stocks:
            print(f"⚠️ Пустой ответ от API\n{url}")

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
        print(f"❌❌  Произошла ошибка при вызове: {e}")
    return data_stocks


if __name__ == '__main__':

    api_key, date = get_api_key()
    stocks_df = get_supplier__stocks(api=api_key, date=date)

    save_in_gsh(df=stocks_df, sheet_name='Остатки (api)')
