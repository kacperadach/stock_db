import datetime

from api.views.quote import get_quote
from core.data.FinvizRepository import Finviz_Repository
from core.data.FuturesRepository import FuturesRepository
from core.data.SymbolRepository import Symbol_Repository
from core.data.uid import decrypt_unique_id, encrypt_unique_id


def fetch_fundamentals(uid):
    return Finviz_Repository.get(uid)

def fetch_metadata(uid):
    symbol = decrypt_unique_id(uid)

    instrument_type = symbol['instrument_type']
    exchange = symbol['exchange']
    sym = symbol['symbol']

    data = Symbol_Repository.get(sym, exchange, instrument_type)

    if instrument_type == 'futures':
        for future in FuturesRepository.get_all_futures():
            if uid == encrypt_unique_id(future):
                data.update(future)
                break

    today = datetime.datetime.now().date()

    prev_days = get_quote(instrument_type, exchange, sym, time_interval='1d', start=today - datetime.timedelta(days=6), end=today + datetime.timedelta(days=1), limit=2)
    if len(prev_days['data']) == 2:
        # data['data'] = prev_days['data']
        if instrument_type == 'futures':
            data.update(prev_days['meta_data'])

        previous = prev_days['data'][0]
        most_recent = prev_days['data'][1]
        # if datetime.strptime(most_recent['date'].split()[0], '%Y-%m-%d').date() == datetime.today().date():
        point_diff = most_recent['close'] - previous['close']
        percentage_diff = point_diff / previous['close'] * 100

        point_diff = '{:.2f}'.format(point_diff)
        percentage_diff = '{:.2f}'.format(percentage_diff)

        data['most_recent_day'] = most_recent['date']
        data['point_diff'] = point_diff
        data['percentage_diff'] = percentage_diff
        data['close'] = most_recent['close']

    return data


if __name__ == '__main__':
    fetch_metadata('SFVCUyAvWE5ZUyAvVVMgIC9zdG9ja3MgICAg')
    fetch_metadata('U0kwMCAvWE5ZTSAvVVMgIC9mdXR1cmVzICAg')
