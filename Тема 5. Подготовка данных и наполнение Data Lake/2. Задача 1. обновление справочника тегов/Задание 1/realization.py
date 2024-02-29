from datetime import datetime, timedelta


def input_paths(date: str, depth: int):
    res = list()
    d = datetime.strptime(date, "%Y-%m-%d").date()
    for i in range(depth):
        dstr = d.strftime("%Y-%m-%d")
        s = f"/user/sergeykhar/data/events/date={dstr}/event_type=message"
        res.append(s)
        d = d - timedelta(days=1)
    return res
