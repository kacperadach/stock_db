import base64

INSTRUMENT_TYPE_MAP = {
    'stocks': 'stocks',
    'rates': 'rates',
    'funds': 'funds',
    'bonds': 'bonds',
    'benchmarks': 'benchmarks',
    'real-estate-investment-trusts': 'reits',
    'futures': 'futures',
    'american-depository-receipt-stocks': 'adrs',
    'exchange-traded-notes': 'etns',
    'warrants': 'warrants',
    'indexes': 'indexes',
    'exchange-traded-funds': 'etfs',
    'currencies': 'currencies',
    'crypto-currencies': 'crypto'
}

def encrypt_unique_id(symbol):
    unique_string = '/'.join(
        ['{:5}'.format(symbol['symbol']), '{:5}'.format(symbol['exchange']), '{:4}'.format(symbol['country_code']), '{:10}'.format(INSTRUMENT_TYPE_MAP.get(symbol['instrument_type']))])
    encoded = base64.urlsafe_b64encode(unique_string)
    return encoded


def decrypt_unique_id(uid):
    print uid
    parts = base64.urlsafe_b64decode(str(uid)).split('/')
    instrument_type = parts[3].strip();

    for key, value in INSTRUMENT_TYPE_MAP.iteritems():
        if value == instrument_type:
            instrument_type = key
            break
    return {'symbol': parts[0].strip(), 'exchange': parts[1].strip(), 'country_code': parts[2].strip(), 'instrument_type': instrument_type, }
