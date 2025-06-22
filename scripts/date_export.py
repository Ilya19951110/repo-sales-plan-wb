from datetime import datetime, timedelta, date


def get_date_range_for_export(today=None):
    today = today or date(2025, 6, 19)

    weekday = today.weekday()

    if weekday == 0:
        start_date = today - timedelta(days=4)
        end_date = today - timedelta(days=1)

    elif weekday == 3:
        start_date = today - timedelta(days=3)
        end_date = today - timedelta(days=1)

    else:
        raise ValueError("⛔ Скрипт нужно запускать только в ПН или ЧТ")

    return start_date, end_date


print(get_date_range_for_export())


def split_period(start_date: str, end_date: str, chunk_days: int = 31) -> list[tuple[str, str]]:

    ranges = []
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while start <= end:
        chunk_end = min(start + timedelta(days=chunk_days - 4), end)

        ranges.append((start.strftime("%Y-%m-%d"),
                      chunk_end.strftime("%Y-%m-%d")))

        start = chunk_end + timedelta(days=1)
    return ranges
