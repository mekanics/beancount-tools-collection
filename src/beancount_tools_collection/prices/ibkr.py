"""
Interactive Brokers (IBKR) price source for Beancount.

This module provides a price source that fetches current market prices from
Interactive Brokers using the IBKR Flex Web Service API. It requires:
- IBKR_TOKEN: Your IBKR Flex Web Service token
- IBKR_QUERY_ID: Your IBKR Flex Query ID
- TZ (optional): Timezone for price timestamps (defaults to "Europe/Zurich")

The source retrieves open positions and their mark prices from IBKR statements.

Note: This script has been copied from somewhere, but I don't know anymore where from.
I'd be happy to reference the author if I know who it is.
"""

from datetime import datetime
from os import environ
from time import sleep

from beancount.core.number import D
from beancount.prices import source
from dateutil import tz
from ibflex import client, parser


class Source(source.Source):
    def get_latest_price(self, ticker: str):
        print("get_latest_price")
        token: str = environ["IBKR_TOKEN"]
        queryId: str = environ["IBKR_QUERY_ID"]

        try:
            response = client.download(token, queryId)
        except client.ResponseCodeError as e:
            if e.code == "1018":
                sleep(10)
                response = client.download(token, queryId)
            else:
                raise e

        statement = parser.parse(response)
        for custStatement in statement.FlexStatements:
            for position in custStatement.OpenPositions:
                if position.symbol.rstrip("z") == ticker:
                    price = D(position.markPrice)
                    timezone = tz.gettz(environ.get("TZ", "Europe/Zurich"))
                    time = datetime.combine(
                        position.reportDate, datetime.min.time()
                    ).astimezone(timezone)

                    return source.SourcePrice(price, time, position.currency)

        return None

    def get_historical_price(self, ticker, time):
        return None
