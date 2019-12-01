
from db.Finance import FinanceDB

if __name__ == "__main__":
    fdb = FinanceDB()
    print('dropping indexes')
    fdb.delete_many("scraper_stats", {})
    fdb.drop_indexes("scraper_stats")
    print('dropped indexes')
