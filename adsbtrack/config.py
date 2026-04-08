from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Config:
    db_path: Path = Path("adsbtrack.db")
    cookies_path: Path = Path("cookies.json")
    airports_csv_url: str = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    adsbx_base_url: str = "https://globe.adsbexchange.com/globe_history"
    rate_limit: float = 0.5  # seconds between requests
    rate_limit_max: float = 30.0  # max backoff after 429s
    rate_limit_recovery: int = 10  # successes before reducing delay
    airport_match_threshold_km: float = 10.0
    airport_types: tuple[str, ...] = ("large_airport", "medium_airport", "small_airport")
