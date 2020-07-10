import singer
from typing import Union, Optional, Dict
from datetime import timedelta, datetime
from dateutil import parser
from tap_capterra.clicks import get_clicks

logger = singer.get_logger()


class Capterra:
    def __init__(self, config: Dict):
        self.config = config
        self.bookmark_key = "date_of_report"

    def stream(self, tap_stream_id: str, state: Optional[datetime] = None):
        prev_bookmark = None
        start_date, end_date = self.__get_start_end(
            state=state, tap_stream_id=tap_stream_id
        )
        with singer.metrics.record_counter(tap_stream_id) as counter:
            try:
                for record in get_clicks(
                    start_date, end_date, self.config.get("api_key")
                ):
                    singer.write_record(tap_stream_id, record)
                    counter.increment(1)
                    new_bookmark = record[self.bookmark_key]
                    if not prev_bookmark:
                        prev_bookmark = new_bookmark

                    if prev_bookmark < new_bookmark:
                        state = self.__advance_bookmark(
                            state=state,
                            bookmark=prev_bookmark,
                            tap_stream_id=tap_stream_id,
                        )
                        prev_bookmark = new_bookmark

            finally:
                self.__advance_bookmark(
                    state=state, bookmark=prev_bookmark, tap_stream_id=tap_stream_id,
                )

    def __get_start_end(self, state: Dict, tap_stream_id: str):
        default_date = (datetime.utcnow() + timedelta(weeks=4)).date()
        end_date = (datetime.utcnow() - timedelta(1)).date()
        logger.info(f"sync data until: {end_date}")

        config_start_date = self.config.get("start_date")
        if config_start_date:
            default_date = parser.isoparse(config_start_date).date()

        if not state:
            logger.info(f"using 'start_date' from config: {default_date}")
            return default_date, end_date

        account_record = state["bookmarks"].get(tap_stream_id, None)
        if not account_record:
            logger.info(f"using 'start_date' from config: {default_date}")
            return default_date, end_date

        current_bookmark = account_record.get(self.bookmark_key, None)
        if not current_bookmark:
            logger.info(f"using 'start_date' from config: {default_date}")
            return default_date, end_date

        state_date = parser.isoparse(current_bookmark)

        # increment by one to not reprocess the previous date
        new_date = (state_date + timedelta(days=1)).date()

        logger.info(f"using 'start_date' from previous state: {current_bookmark}")
        return new_date, end_date

    def __advance_bookmark(
        self, state: dict, bookmark: Union[str, datetime, None], tap_stream_id: str
    ):
        if not bookmark:
            singer.write_state(state)
            return state

        if isinstance(bookmark, datetime):
            bookmark_datetime = bookmark.date()
        elif isinstance(bookmark, str):
            bookmark_datetime = parser.isoparse(bookmark).date()
        else:
            raise ValueError(
                f"bookmark is of type {type(bookmark)} but must be either string or datetime"
            )

        state = singer.write_bookmark(
            state, tap_stream_id, self.bookmark_key, bookmark_datetime.isoformat()
        )
        singer.write_state(state)
        return state

