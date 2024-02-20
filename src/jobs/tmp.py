from datetime import datetime, timedelta
from jobs.daily_score import main

date = datetime(2024, 2, 9)
while date >= datetime(2024, 1, 28):
    date_str = date.strftime("%Y%m%d")
    print(f"---------- {date_str} start!! ----------")
    main(date_str, 'ko', 'google_suggest_for_trend')
    date = date - timedelta(days=1)