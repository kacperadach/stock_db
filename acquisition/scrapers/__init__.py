from acquisition.scrapers.BarchartFinancialsScraper import BarchartFinancialsScraper
from acquisition.scrapers.BarchartOptionsScraper import BarchartOptionsScraper
from acquisition.scrapers.BondScraper import BondScraper
from acquisition.scrapers.FxstreetScraper import FxstreetScraper
from acquisition.scrapers.NasdaqOptionsScraper import NasdaqOptionsScraper
from .FuturesScraper import FuturesScraper, Futures1mScraper
from .IndexLiveScraper import IndexLiveScraper
from .MarketWatchHistoricalScraper import MarketWatchHistoricalScraper
from acquisition.scrapers.FinvizScraper import FinvizScraper
from acquisition.scrapers.MarketWatchSymbolsV2 import MarketWatchSymbolsV2
from acquisition.scrapers.MarketWatchLiveScraper import MarketWatchLiveScraper
from acquisition.scrapers.MarketWatchRequestLiveScraper import MarketWatchRequestLiveScraper
from acquisition.scrapers.RandomMarketWatchSymbols import RandomMarketWatchSymbols

ALL_SCRAPERS = (
    FuturesScraper,
    Futures1mScraper,
    IndexLiveScraper,
    MarketWatchHistoricalScraper,
    MarketWatchLiveScraper,
    MarketWatchRequestLiveScraper,
    MarketWatchSymbolsV2,
    RandomMarketWatchSymbols,
    FinvizScraper,
    BondScraper,
    FxstreetScraper,
    NasdaqOptionsScraper,
    BarchartFinancialsScraper,
    BarchartOptionsScraper
)