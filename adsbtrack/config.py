from dataclasses import dataclass, field
from pathlib import Path


SOURCE_URLS = {
    "adsbx": "https://globe.adsbexchange.com/globe_history",
    "adsbfi": "https://globe.adsb.fi/globe_history",
    "airplaneslive": "https://globe.airplanes.live/globe_history",
    "adsblol": "https://adsb.lol/globe_history",
    "theairtraffic": "https://globe.theairtraffic.com/globe_history",
}


@dataclass
class Config:
    db_path: Path = Path("adsbtrack.db")
    cookies_path: Path = Path("cookies.json")
    credentials_path: Path = Path("credentials.json")
    airports_csv_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    rate_limit: float = 0.5  # seconds between requests
    rate_limit_max: float = 30.0  # max backoff after 429s
    rate_limit_recovery: int = 10  # successes before reducing delay
    airport_match_threshold_km: float = 10.0
    airport_types: tuple[str, ...] = ("large_airport", "medium_airport", "small_airport")
