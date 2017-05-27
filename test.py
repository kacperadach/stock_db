from multiprocessing.pool import ThreadPool as Pool
from datetime import datetime


from acquisition.daily.options.options import get_all_options_data, get_options_data
from acquisition.symbol.tickers import StockTickers


def single_approach(tickers):
    d = []
    for t in tickers:
        d.append(get_options_data(t))
    return d


def multi_threaded_approach(tickers, pool_size):
    d = []
    pool = Pool(pool_size)
    for t in tickers:
        d.append(pool.apply_async(get_options_data, (t,)).get())
        #d.append(get_options_data(t))
    pool.close()
    pool.join()
    return d

if __name__ == "__main__":
    tickers = StockTickers().get_all()[3000:4000]
    start = datetime.now()
    data = multi_threaded_approach(tickers[0:500], 100)
    end = datetime.now()
    print 'time taken: {}'.format(end-start)
    start = datetime.now()
    data = single_approach(tickers[500:1000])
    end = datetime.now()
    print 'time taken: {}'.format(end - start)