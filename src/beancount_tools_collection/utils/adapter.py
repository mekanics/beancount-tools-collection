from beangulp.importer import Importer


class ImporterProtocolAdapter(Importer):
    def __init__(self, adaptee: Importer):
        self.adaptee = adaptee

    def identify(self, f):
        return self.adaptee.identify(f)

    def extract(self, f):
        return self.adaptee.extract(f)

    def file_account(self, f):
        return self.adaptee.file_account(f) if hasattr(self.adaptee, 'file_account') else None

    def file_name(self, f):
        return self.adaptee.file_name(f) if hasattr(self.adaptee, 'file_name') else None

    def file_date(self, f):
        return self.adaptee.file_date(f) if hasattr(self.adaptee, 'file_date') else None
