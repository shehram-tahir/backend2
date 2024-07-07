from typing import Dict, List, TypeVar, Generic, Literal

from pydantic import BaseModel


# class ResDefault(BaseModel):
#     message: str
#     request_id: str


class card_metadata(BaseModel):
    id: str
    name: str
    description: str
    thumbnail_url: str
    catalog_link: str
    records_number: int
    can_access: int


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


class CityData(BaseModel):
    name: str
    lat: float
    lng: float
    radius: int
    type: str = None


class DataCreateLyr(BaseModel):
    type: Literal["FeatureCollection"]
    features: List[Feature]
    bknd_dataset_id: str
    records_count: int


class LayerInfo(BaseModel):
    prdcer_lyr_id: str
    prdcer_layer_name: str
    points_color: str
    layer_legend: str
    layer_description: str
    records_count: int
    is_zone_lyr: str


class PrdcerLyrMapData(MapData):
    prdcer_layer_name: str
    prdcer_lyr_id: str
    bknd_dataset_id: str
    points_color: str
    layer_legend: str
    layer_description: str
    records_count: int
    is_zone_lyr: str


class UserCatalogInfo(BaseModel):
    prdcer_ctlg_id: str
    prdcer_ctlg_name: str
    ctlg_description: str
    thumbnail_url: str
    subscription_price: str
    total_records: int
    lyrs: List[str]
    ctlg_owner_user_id: str


class ZoneLayerInfo(BaseModel):
    lyr_id: str
    property_key: str


# Request models
class ReqLocation(BaseModel):
    lat: float
    lng: float
    radius: int
    type: str


class ReqCatalogId(BaseModel):
    catalogue_dataset_id: str


class ReqUserId(BaseModel):
    user_id: str


class ReqPrdcerLyrMapData(BaseModel):
    prdcer_lyr_id: str
    user_id: str


class ReqCreateLyr(BaseModel):
    dataset_category: str
    dataset_country: str
    dataset_city: str


class ReqSavePrdcerLyer(BaseModel):
    prdcer_layer_name: str
    prdcer_lyr_id: str
    bknd_dataset_id: str
    points_color: str
    layer_legend: str
    layer_description: str
    user_id: str


class ReqApplyZoneLayers(BaseModel):
    user_id: str
    lyrs: List[str]
    lyrs_as_zone: List[Dict[str, str]]


class ReqCreateUserProfile(BaseModel):
    user_id: str
    username: str
    email: str


class ReqFetchCtlgLyrs(BaseModel):
    prdcer_ctlg_id: str
    as_layers: bool
    user_id: str


class ReqSavePrdcerCtlg(BaseModel):
    prdcer_ctlg_name: str
    prdcer_ctlg_id: str
    subscription_price: str
    ctlg_description: str
    total_records: int
    lyrs: List[str]
    user_id: str
    thumbnail_url: str


T = TypeVar('T')
U = TypeVar('U')


class ResponseModel(BaseModel, Generic[T]):
    message: str
    request_id: str
    data: T


class RequestModel(BaseModel, Generic[U]):
    message: str
    request_info: Dict
    request_body: U



# Refactored response models
ResAllCards = ResponseModel[List[card_metadata]]
ResUserLayers = ResponseModel[List[LayerInfo]]
ResCtlgLyrs = ResponseModel[List[PrdcerLyrMapData]]
ResApplyZoneLayers = ResponseModel[List[PrdcerLyrMapData]]
ResCreateUserProfile = ResponseModel[str]
ResAcknowlg = ResponseModel[str]
ResSavePrdcerCtlg = ResponseModel[str]
ResTypeMapData = ResponseModel[MapData]
ResCountryCityData = ResponseModel[Dict[str, List[CityData]]]
ResNearbyCategories = ResponseModel[Dict[str, List[str]]]
ResPrdcerLyrMapData = ResponseModel[PrdcerLyrMapData]
ResCreateLyr = ResponseModel[DataCreateLyr]
ResOldNearbyCategories = ResponseModel[List[str]]
ResUserCatalogs = ResponseModel[List[UserCatalogInfo]]

# # Refactored request models
# ReqLocation = RequestModel[ReqLocation]
# ReqCatalogId = RequestModel[ReqCatalogId]
# ReqUserId = RequestModel[ReqUserId]
# ReqPrdcerLyrMapData = RequestModel[ReqPrdcerLyrMapData]
# ReqCreateLyr = RequestModel[ReqCreateLyr]
# ReqSavePrdcerLyer = RequestModel[ReqSavePrdcerLyer]
# ReqApplyZoneLayers = RequestModel[ReqApplyZoneLayers]
# ReqCreateUserProfile = RequestModel[ReqCreateUserProfile]
# ReqFetchCtlgLyrs = RequestModel[ReqFetchCtlgLyrs]
# ReqSavePrdcerCtlg = RequestModel[ReqSavePrdcerCtlg]
