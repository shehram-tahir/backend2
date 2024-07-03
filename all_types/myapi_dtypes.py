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


class ResAcknowlg(ResDefault):
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
    type: Literal["Point"]
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
    type: Literal["Feature"]
    properties: dict
    geometry: Geometry


class MapData(BaseModel):
    type: Literal["FeatureCollection"]
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


class ReqCreateLyr(BaseModel):
    dataset_category: str
    dataset_country: str
    dataset_city: str


class DataCreateLyr(BaseModel):
    type: Literal["FeatureCollection"]
    features: List[Feature]
    bknd_dataset_id: str
    records_count: int


class ResCreateLyr(ResDefault):
    data: DataCreateLyr


class ReqSavePrdcerLyer(BaseModel):
    prdcer_layer_name: str
    prdcer_lyr_id: str
    bknd_dataset_id: str
    points_color: str
    layer_legend: str
    layer_description: str
    user_id: str


class UserIdRequest(BaseModel):
    user_id: str


class UserLayerInfo(BaseModel):
    prdcer_lyr_id: str
    prdcer_layer_name: str
    points_color: str
    layer_legend: str
    layer_description: str
    records_count: int
    is_zone_lyr: str


class UserLayersResponse(ResDefault):
    data: List[UserLayerInfo]


class ReqPrdcerLyrMapData(BaseModel):
    prdcer_lyr_id: str
    user_id: str


class PrdcerLyrMapData(MapData):
    prdcer_layer_name: str
    prdcer_lyr_id: str
    bknd_dataset_id: str
    points_color: str
    layer_legend: str
    layer_description: str
    records_count: int
    is_zone_lyr: str


class ResPrdcerLyrMapData(ResDefault):
    data: PrdcerLyrMapData


class ReqSavePrdcerCtlg(BaseModel):
    prdcer_ctlg_name: str
    prdcer_ctlg_id: str
    subscription_price: str
    ctlg_description: str
    total_records: int
    lyrs: List[str]
    user_id: str  # Add this field to identify the user
    thumbnail_url: str  # Add this line


class ResSavePrdcerCtlg(ResDefault):
    data: str

class UserCatalogInfo(BaseModel):
    prdcer_ctlg_id: str
    prdcer_ctlg_name: str
    ctlg_description: str
    thumbnail_url: str
    subscription_price: str
    total_records: int
    lyrs: List[str]

class UserCatalogsResponse(ResDefault):
    data: List[UserCatalogInfo]



class ReqFetchCtlgLyrs(BaseModel):
    prdcer_ctlg_id: str
    as_layers: bool
    user_id: str  # Add this to identify the user

class CtlgLyrInfo(PrdcerLyrMapData):
    pass

class ResCtlgLyrs(ResDefault):
    data: List[CtlgLyrInfo]








