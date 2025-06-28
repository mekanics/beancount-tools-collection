"""
Example: Basic Transaction Processing with ImporterProtocolAdapter and TransactionInspector

This example demonstrates how to use ImporterProtocolAdapter to wrap an existing importer
and TransactionInspector to modify transactions during import.
"""

from .adapter import ImporterProtocolAdapter
from beancount.core import data
from .transactionInspector import TransactionInspector


class BasicTransactionProcessor(ImporterProtocolAdapter):
    """
    Example importer that processes transactions using TransactionInspector.
    
    This class wraps any existing importer and applies basic transaction modifications:
    - Standardizes payee names
    - Adds appropriate expense categories
    - Flags certain transactions for review
    """
    
    def __init__(self, base_importer):
        """
        Initialize with a base importer to wrap.
        
        Args:
            base_importer: Any importer that implements the beangulp.importer.Importer interface
        """
        super().__init__(base_importer)

    def extract(self, f):
        """
        Extract transactions from file and apply modifications.
        
        Args:
            f: File object to process
            
        Returns:
            List of processed beancount entries
        """
        # Get entries from the wrapped importer
        entries = super().extract(f)
        result = []

        for entry in entries:
            if isinstance(entry, data.Transaction):
                # Process transactions using TransactionInspector
                processed = self._process_transaction(entry)
                if processed:  # Only add if not filtered out
                    result.append(processed)
            else:
                # Pass through non-transaction entries unchanged
                result.append(entry)

        return result

    def _process_transaction(self, transaction: data.Transaction) -> data.Transaction:
        """
        Process a single transaction using TransactionInspector.
        
        Args:
            transaction: The transaction to process
            
        Returns:
            Modified transaction, or None to filter it out
        """
        tx = TransactionInspector(transaction)

        # Example 1: Standardize food delivery service names
        if tx.hasPayee("Uber eats"):
            tx.replacePayee("Uber Eats").simplePosting("Expenses:Food:Delivery")
        
        elif tx.hasPayee("JustEat"):
            tx.replacePayee("Just Eat").simplePosting("Expenses:Food:Delivery")

        # Example 2: Categorize Swiss shopping
        elif tx.hasPayee("Digitec Galaxus"):
            tx.simplePosting("Expenses:Shopping:Electronics")
            
        elif tx.hasPayee("Migros") or tx.hasPayee("Coop"):
            tx.simplePosting("Expenses:Food:Groceries")

        # Example 3: Handle Swiss salary deposits
        elif tx.hasPayee("UBS") and tx.isCredit():
            tx.replacePayee("UBS Switzerland AG").simplePosting("Income:Salary")

        # Example 4: Flag large transactions for review
        if tx.hasFirstPostingWithLessThan(-1000):  # Expenses > CHF 1000
            tx.flagWarning()

        return tx.transaction


# Example usage:
"""
To use this processor with an existing importer:

from your_importers import SwissBankImporter
from beancount_tools_collection.utils.magic import BasicTransactionProcessor

# Wrap your existing importer
base_importer = SwissBankImporter("Assets:UBS:Checking")
processor = BasicTransactionProcessor(base_importer)

# Use in your import configuration
CONFIG = [processor]
"""
