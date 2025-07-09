from .adapter import ImporterProtocolAdapter
from beancount.core.number import D
from beancount.core import account
from beancount.core import amount
from beancount.core import flags
from beancount.core import data
from beancount.core.position import Cost, CostSpec


class TransactionInspector:
    def __init__(self, transaction: data.Transaction):
        self.transaction = transaction

    def hasPayee(self, payee: str) -> bool:
        return payee.lower() in self.transaction.payee.lower()

    def isDebit(self) -> bool:
        return self.transaction.postings[0].units.number < amount.Decimal("0")

    def isCredit(self) -> bool:
        return not self.isDebit()

    def hasFirstPostingWithLessThan(self, x: amount.Decimal) -> bool:
        return self.transaction.postings[0].units.number < x

    def hasFirstPostingWith(self, x: amount.Decimal) -> bool:
        return self.transaction.postings[0].units.number == x

    def replacePayee(self, newPayee: str):
        self.transaction = self.transaction._replace(
            narration=self.transaction.payee,
            payee=newPayee
        )
        return self

    def narration(self, narration: str):
        self.transaction = self.transaction._replace(
            narration=narration
        )
        return self

    def flagOkay(self):
        self.transaction = self.transaction._replace(
            flag=flags.FLAG_OKAY
        )
        return self

    def flagWarning(self):
        self.transaction = self.transaction._replace(
            flag=flags.FLAG_WARNING
        )
        return self

    def simplePosting(self, account, units: amount.Amount | None = None):
        self.transaction.postings.append(data.Posting(
            account,
            units, 
            None, None, None, None
        ))
        return self

    def addTag(self, tag: str):
        self.transaction = self.transaction._replace(
            tags=self.transaction.tags.union([tag])
        )
        return self

    def addTags(self, tags: list[str]):
        self.transaction = self.transaction._replace(
            tags=self.transaction.tags.union(tags))

    def addLink(self, link: str):
        self.transaction = self.transaction._replace(
            links=self.transaction.links.union([link])
        )
        return self

    def addLinks(self, links: list[str]):
        self.transaction = self.transaction._replace(
            links=self.transaction.links.union(links))