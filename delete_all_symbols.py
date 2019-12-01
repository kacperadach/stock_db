from core.data.SymbolRepository import Symbol_Repository
from db.Finance import FinanceDB

if __name__ == "__main__":
    print('deleting')
    Symbol_Repository.delete_many({})
    print('deleted')

    print('dropping indexes')
    FinanceDB().drop_indexes("market_watch_symbols")
    print('dropped indexes')
