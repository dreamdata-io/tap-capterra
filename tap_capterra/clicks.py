import requests
from ratelimit import limits
from dateutil.rrule import rrule, DAILY
import singer
from requests.exceptions import HTTPError
from tqdm import tqdm

SESSION = requests.Session()
FIVE_MINUTES = 300
logger = singer.get_logger()


def get_clicks(start_date, end_date, api_key):
    params = None
    if start_date > end_date:
        logger.error(f"start date: {start_date} is larger than end date: {end_date} ")
        return
    kwargs = {
        "total": len(list(rrule(DAILY, dtstart=start_date, until=end_date))),
        "unit": "day",
    }
    for date in tqdm(rrule(DAILY, dtstart=start_date, until=end_date), **kwargs):
        params = {"start_date": date.date(), "end_date": date.date()}
        while True:
            data, scroll_id = call_api(params, api_key)
            params = {"scroll_id": scroll_id}
            if data:
                yield from data

            if not scroll_id:
                break


@limits(calls=5000, period=FIVE_MINUTES)
def call_api(params, api_key):
    try:
        response = SESSION.get(
            "https://public-api.capterra.com/v1/clicks",
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            params=params,
        )
        response.raise_for_status()
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
    response = response.json()
    data = response["data"]
    scroll_id = response.get("scroll_id", None)
    return data, scroll_id

