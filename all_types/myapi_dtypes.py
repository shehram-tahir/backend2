from typing import Dict, List, Literal, Tuple
from pydantic import BaseModel, Field


class LocationReq(BaseModel):
    lat: float
    lng: float
    radius: int
    type: str


class CatlogId(BaseModel):
    catalogue_dataset_id: str


class ResDefault(BaseModel):
    message: str
    request_id: str


class restype_fetch_acknowlg_id(ResDefault):
    data: str







class card_metadata(BaseModel):
    id: str
    name: str
    description: str
    thumbnail_url: str
    catalog_link: str
    records_number: int
    can_access: int


class restype_all_cards(ResDefault):
    message: str
    request_id: str
    data: list[card_metadata]



class Geometry(BaseModel):
    type: Literal['Point']
    coordinates: List[float]


class boxmapProperties(BaseModel):
    name: str
    rating: float
    address: str
    phone: str
    website: str
    business_status: str
    user_ratings_total: int


class Feature(BaseModel):
    type: Literal['Feature']
    properties: dict
    geometry: Geometry

class MapData(BaseModel):
    type: Literal['FeatureCollection']
    features: List[Feature]


class ResTypeMapData(ResDefault):
    data: MapData

class CityData(BaseModel):
    name: str
    lat: float
    lng: float
    radius: int
    type: str = None

class CountryCityData(BaseModel):
    data: Dict[str, List[CityData]]


class NearbyCategories(ResDefault):
    data: Dict[str, List[str]]

class OldNearbyCategories(ResDefault):
    data: List[str]