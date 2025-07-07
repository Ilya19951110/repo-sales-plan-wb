import argparse


def parse_args():
    parser = argparse.ArgumentParser(description='Запуск выгрузки по апи')

    parser.add_argument(
        "--cabinet",
        type=str,
        help="Имя кабинета, который нужно выгрузить (например, 'Фин модель Галилова')"
    )

    return parser.parse_args()
