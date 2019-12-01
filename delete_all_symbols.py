from core.data.SymbolRepository import SymbolRepository
from db.Finance import FinanceDB

if __name__ == "__main__":
    print('deleting')
    SymbolRepository().delete_many({})
    print('deleted')

    print('dropping indexes')
    FinanceDB().drop_indexes("market_watch_symbols")
    print('dropped indexes')
