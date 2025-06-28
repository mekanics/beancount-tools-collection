import csv
import logging
from datetime import timedelta
from io import StringIO

from beancount.core import amount, data
from beancount.core.number import D
from beangulp.importer import Importer
from dateutil.parser import parse


class FireFlyImporter(Importer):
    """An importer for firefly-iii exports."""

    def identify(self, f):
        if 'firefly' in f.name:
            return True

        return False

    def extract(self, filepath, existing=None):
        entries = dict()

        with StringIO(filepath.contents()) as csvfile:
            reader = csv.DictReader(
                csvfile,
                delimiter=",",
                skipinitialspace=True,
            )

            for row in reader:
                try:
                    amount_raw = D(row["amount"].strip())
                    amt = amount.Amount(amount_raw, row["currency_code"])
                    
                    book_date = parse(row["date"].strip()).date()
                    tx_id = D(row["group_id"])
                except Exception as e:
                    logging.warning(e)
                    continue


                if tx_id in entries: 
                    # entries[tx_id].postings.append(
                    #     data.Posting(
                    #         tx_transfer,
                    #         amount.Amount(tx_amount, tx_currency),
                    #         None,
                    #         None,
                    #         None,
                    #         None,
                    #     )
                    # )
                    # entries[tx_id] = entries[tx_id]._replace(
                    #     narration=(" | ").join(
                    #         filter(None, (entries[tx_id].narration, tx_narration))
                    #     )
                    # )
                    print("TODO")
                else:
                    entry = data.Transaction(
                        data.new_metadata(filepath, 0, {}),
                        book_date,
                        "*",
                        "",
                        row["Description"].strip(),
                        data.EMPTY_SET,
                        data.EMPTY_SET,
                        [
                            data.Posting(self.account, amt, None, None, None, None),
                            data.Posting(self.account, amt, None, None, None, None),
                        ],
                    )
                    entries.append(entry)