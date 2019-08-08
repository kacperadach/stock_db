from core.data.ScraperRepository import Scraper_Repository

def get_stats():
    return Scraper_Repository.get_recent()