from dataclasses import dataclass


@dataclass
class ApiCommonConfig:
    api_key: str
    base_urls: dict[str]


@dataclass
class ApiRequestConfig:
    nearby_search: dict[str]
    place_details: dict[str]


@dataclass
class DbConfig:
    db_connection_string: str
    db_name: str