from yfk.insider_transactions import InsiderTransactions
from acquisition.daily.insider import get_all_insider_data

a = InsiderTransactions('sgyp')
print a.get_data()