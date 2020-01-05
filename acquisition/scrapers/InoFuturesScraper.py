from copy import deepcopy
from datetime import datetime, timedelta

from core.BaseScraper import BaseScraper
from core.QueueItem import QueueItem
from core.data.InoFuturesRepository import InoFuturesRepository
from request.InoFuturesRequest import InoFuturesAllContractsRequest, InoFuturesContractsRequest, InoFuturesRequest, InoFuturesOptionsChainRequest, DATA_PERIODS


class InoFuturesScraper(BaseScraper):

    def __init__(self):
        super(InoFuturesScraper, self).__init__()
        self.ino_futures_repository = InoFuturesRepository()

    def get_time_delta_for_period(self, period):
        if period == 'history':
            return timedelta(days=60)
        elif period == 'hour':
            return timedelta(days=15)
        elif period == 'minute':
            return timedelta(days=5)

    def get_symbols(self):
        return [{'type': 'all'}]

    def get_queue_item(self, symbol):
        timeout = 10
        if 'retry' in symbol:
            timeout = 30

        if symbol['type'] == 'all':
            return QueueItem.from_request(InoFuturesAllContractsRequest(), __name__, symbol, timeout)
        elif symbol['type'] == 'contract_type':
            return QueueItem.from_request(InoFuturesContractsRequest(symbol['contract_type']), __name__, symbol, timeout)
        elif symbol['type'] == 'contract':
            request = InoFuturesRequest(symbol['contract']['contract_link'], symbol['period'], symbol['start'].strftime('%Y-%m-%d'), symbol['end'].strftime('%Y-%m-%d'))
            return QueueItem.from_request(request, __name__, symbol, timeout)
        elif symbol['type'] == 'options_chain':
            return QueueItem.from_request(InoFuturesOptionsChainRequest(symbol['contract']['contract_link']), __name__, symbol, timeout)
        elif symbol['type'] == 'option':
            return QueueItem.from_request(InoFuturesRequest(symbol['option']['symbol_link'], 'history'), None, symbol, timeout)

    def get_time_delta(self):
        return timedelta(hours=12)

    def process_data(self, queue_item):
        type = queue_item.get_metadata()['type']
        if type not in ('contract', 'option'):
            return

        data = InoFuturesRequest.parse_response(queue_item.get_response().get_data())
        if not data:
            return

        if type == 'contract':
            self.ino_futures_repository.insert_future(data, queue_item.get_metadata())
        elif type == 'option':
            self.ino_futures_repository.insert_option(data, queue_item.get_metadata())

    def should_scrape(self):
        return True

    def requests_per_second(self):
        return 4

    def request_callback(self, queue_item):
        meta = queue_item.get_metadata()
        if not queue_item.get_response().is_successful():
            if meta['type'] in ('all', 'contract_type') and 'retry' not in meta:
                new_meta = deepcopy(meta)
                new_meta['retry'] = True
                self.additional_symbols.append(new_meta)
            return

        if meta['type'] == 'all':
            all_contract_types = InoFuturesAllContractsRequest.parse_response(queue_item.get_response().get_data())
            for contract_type in all_contract_types:
                self.additional_symbols.append({
                    'type': 'contract_type',
                    'contract_type': contract_type
                })
        elif meta['type'] == 'contract_type':
            contracts = InoFuturesContractsRequest.parse_response(queue_item.get_response().get_data())
            end = datetime.utcnow()
            for contract in contracts:
                for period in DATA_PERIODS:
                    self.additional_symbols.append({
                        'contract_type': meta['contract_type'],
                        'type': 'contract',
                        'contract': contract,
                        'period': period,
                        'start': end - self.get_time_delta_for_period(period),
                        'end': end
                    })
                self.additional_symbols.append({
                    'type': 'options_chain',
                    'contract': contract
                })
        elif meta['type'] == 'contract':
            if queue_item.get_response().get_data():
                new_end = datetime.utcfromtimestamp(queue_item.get_response().get_data()[0][0] / 1000)
                oldest_date = self.ino_futures_repository.get_oldest_future_date(meta['contract']['contract_link'], meta['period'])
                if oldest_date and oldest_date[0]['trading_date'] < new_end:
                    new_end = oldest_date[0]['trading_date']

                if meta['end'] < new_end:
                    return

                additional = deepcopy(meta)
                additional['end'] = new_end - timedelta(days=1)
                additional['start'] = additional['end'] - self.get_time_delta_for_period(meta['period'])
                self.additional_symbols.append(additional)
        elif meta['type'] == 'options_chain':
            options = InoFuturesOptionsChainRequest.parse_response(queue_item.get_response().get_data())
            for option in options:
                self.additional_symbols.append({
                    'type': 'option',
                    'contract': queue_item.get_metadata()['contract'],
                    'option': option
                })
