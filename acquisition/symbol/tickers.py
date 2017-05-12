from os import path
from ftplib import FTP

from IPython.core.debugger import Tracer

SPECIFIC_PATH = 'all_tickers.txt'
DIRNAME, _ = path.split(path.abspath(__file__))
BASE_PATH = path.dirname(path.abspath(__file__))

FTP_ADDRESS = 'ftp.nasdaqtrader.com'

FTP_CWD = 'SymbolDirectory'

NASDAQ_FILE = 'nasdaqlisted.txt'

OTHER_FILE = 'otherlisted.txt'

FTP_DELIMITER = '|'

IGNORED_STRINGS = ('SYMBOL', )

FILTERED_SYMBOLS = ('^', '.', '$')

def get_all_tickers_from_ftp(base=BASE_PATH, specific=SPECIFIC_PATH):
    _write_all_tickers_from_ftp(base, specific)
    all_tickers = _pull_all_tickers_from_file(base, specific)
    #logger.info("Found {} tickers using ftp".format(len(all_tickers)))
    return _filter_all_tickers(all_tickers)

def _pull_all_tickers_from_file(base, specific):
	ticker_file = open(path.join(base, specific), 'r')
	all_tickers = []
	for line in ticker_file:
	    if FTP_DELIMITER in line:
	        ticker = line.split(FTP_DELIMITER)[0]
	        if ticker.upper() not in IGNORED_STRINGS:
	            all_tickers.append(ticker)
	return all_tickers

def _write_all_tickers_from_ftp(base, specific):
    try:
        ticker_file = open(path.join(base, specific), 'wb')
        #Tracer()()
        ftp = FTP(FTP_ADDRESS)
        ftp.login()
        Tracer()()
        ftp.cwd(FTP_CWD)
        ftp.retrbinary('RETR {}'.format(NASDAQ_FILE), ticker_file.write)
        ftp.retrbinary('RETR {}'.format(OTHER_FILE), ticker_file.write)
    except:
    	a = 1
       # logger.error("Error retrieving tickers from NASDAQ ftp")

def _filter_all_tickers(all_tickers):
    filtered_list = []
    for ticker in all_tickers:
        if not any(sym in ticker for sym in FILTERED_SYMBOLS):
            filtered_list.append(ticker)
    filtered_list.sort()
    return filtered_list