from db.Finance import FinanceDB

if __name__ == "__main__":
    f = FinanceDB("test")
    f.insert_one({"test": 1})