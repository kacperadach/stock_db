from abc import abstractmethod

class BaseSymbol():

    @abstractmethod
    def get_symbol(self):
        raise NotImplementedError('get_symbol')

    @abstractmethod
    def get_instrument_type(self):
        raise NotImplementedError('get_instrument_type')

    @abstractmethod
    def get_country(self):
        raise NotImplementedError('get_country')

    @abstractmethod
    def get_exchange(self):
        raise NotImplementedError('get_exchange')





