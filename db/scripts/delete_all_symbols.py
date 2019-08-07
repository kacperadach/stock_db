from core.data.SymbolRepository import Symbol_Repository
from db.Finance import Finance_DB

if __name__ == "__main__":
    print 'deleting'
    Symbol_Repository.delete_many({})
    print 'deleted'

    print 'dropping indexes'
    Finance_DB.drop_indexes("market_watch_symbols")
    print 'dropped indexes'
