from core.data.SymbolRepository import Symbol_Repository
from db.Finance import Finance_DB

if __name__ == "__main__":
    print 'dropping indexes'
    Finance_DB.drop_indexes("scraper_stats")
    print 'dropped indexes'
