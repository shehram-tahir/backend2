from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ValidationError
import uuid
from all_types.config_dtypes import ApiCommonConfig
from all_types.myapi_dtypes import UserCatalogsResponse
from data_fetcher import (
    get_boxmap_catlog_data,
    fetch_catlog_collection, 
    nearby_boxmap, 
    fetch_country_city_data,
    fetch_nearby_categories,
    fetch_layer_collection,
    old_fetch_nearby_categories,
    fetch_or_create_lyr,
    create_save_prdcer_lyr,
    fetch_prdcer_lyrs,
    fetch_prdcer_lyr_map_data,
    create_save_prdcer_ctlg,
    fetch_prdcer_ctlgs
      )
from all_types.myapi_dtypes import (
    LocationReq,
    CatlogId,
    restype_all_cards,
    ResAcknowlg,
    ResTypeMapData,
    CountryCityData,
    NearbyCategories,
    OldNearbyCategories,
    ReqCreateLyr,
    ResCreateLyr,
    ReqSavePrdcerLyer,
    ReqPrdcerLyrMapData,
    ResPrdcerLyrMapData
)
from all_types.myapi_dtypes import UserIdRequest, UserLayersResponse,ReqSavePrdcerCtlg, ResSavePrdcerCtlg

from all_types.myapi_dtypes import MapData
from typing import Type, Callable, Awaitable, Any, Optional
from config_factory import get_conf

CONF = get_conf()


app = FastAPI()

# Enable CORS
origins = [CONF.enable_CORS_url]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, websocket: WebSocket, request_id: str):
        await websocket.accept()
        self.active_connections[request_id] = websocket

    def disconnect(self, request_id: str):
        if request_id in self.active_connections:
            del self.active_connections[request_id]

    async def send_personal_message(self, message: str, request_id: str):
        if request_id in self.active_connections:
            await self.active_connections[request_id].send_text(message)

    async def send_json(self, data: dict, request_id: str):
        if request_id in self.active_connections:
            await self.active_connections[request_id].send_json(data)


manager = ConnectionManager()


async def ws_handling(
    websocket: WebSocket,
    request_id: str,
    input_type: Type[BaseModel],
    output_type: Type[BaseModel],
    custom_function: Callable[[Any], Awaitable[Any]]
):
    await manager.connect(websocket, request_id)
    try:
        while True:
            req = await websocket.receive_text()
            parsed_req = input_type.model_validate_json(req)
            response = await custom_function(parsed_req)
            try:
                validated_output = output_type.model_validate(response)
                await manager.send_json(
                    {"data": validated_output.model_dump()}, request_id
                )
            except ValidationError as e:
                error_message = f"Output data validation failed: {str(e)}"
                await manager.send_json({"error": error_message}, request_id)
    except WebSocketDisconnect:
        manager.disconnect(request_id)
        print(f"WebSocket disconnected: {request_id}")


async def http_handling(
    req: Optional[BaseModel],
    input_type: BaseModel,
    output_type: BaseModel,
    custom_function: Callable[[BaseModel], Any],
):
    output = ""

    if req is not None:
        try:
            input_type.model_validate(req)
        except ValidationError as e:
            raise HTTPException(
                status_code=400, detail=f"Invalid request body with error: {e}"
            ) from e

    if custom_function is not None:
        output = await custom_function(req=req)

    request_id = "req-" + str(uuid.uuid4())

    
    res_body = output_type(
        message="Request received",
        request_id=request_id,
        data=output,
    )

    return res_body


# @app.websocket(CONF.catlog_data)
# async def catlog_data(websocket: WebSocket, request_id: str):
#     await ws_handling(
#         websocket,
#         request_id,
#         CatlogId,
#         MapData,
#         get_boxmap_catlog_data,
#     )

@app.post(CONF.http_catlog_data, response_model=ResTypeMapData)
async def catlog_data(catlog_req:CatlogId):
    response = await http_handling(
        catlog_req,
        CatlogId,
        ResTypeMapData,
        get_boxmap_catlog_data,
    )
    return response

# @app.websocket(CONF.single_nearby)
# async def single_nearby(websocket: WebSocket, request_id: str):
#     await ws_handling(websocket, request_id, LocationReq, CatlogData, nearby_boxmap)


@app.post(CONF.http_single_nearby, response_model=ResTypeMapData)
async def single_nearby(req:LocationReq):
    response = await http_handling(
        req,
        LocationReq,
        ResTypeMapData,
        nearby_boxmap,
    )
    return response

@app.get(CONF.fetch_acknowlg_id, response_model=ResAcknowlg)
async def fetch_acknowlg_id():
    response = await http_handling(
        None,
        None,
        ResAcknowlg,
        None,
    )
    return response



@app.get(CONF.catlog_collection, response_model=restype_all_cards)
async def catlog_collection():
    response = await http_handling(
        None,
        None,
        restype_all_cards,
        fetch_catlog_collection,
    )
    return response

@app.get(CONF.layer_collection, response_model=restype_all_cards)
async def layer_collection():
    response = await http_handling(
        None,
        None,
        restype_all_cards,
        fetch_layer_collection,
    )
    return response



@app.get(CONF.country_city, response_model=CountryCityData)
async def country_city():
    response = await http_handling(
        None,
        None,
        CountryCityData,
        fetch_country_city_data,
    )
    return response


@app.get(CONF.nearby_categories, response_model=NearbyCategories)
async def nearby_categories():
    response = await http_handling(
        None,
        None,
        NearbyCategories,
        fetch_nearby_categories,
    )
    return response

@app.get(CONF.old_nearby_categories, response_model=OldNearbyCategories)
async def old_nearby_categories():
    response = await http_handling(
        None,
        None,
        OldNearbyCategories,
        old_fetch_nearby_categories,
    )
    return response


@app.post(CONF.create_layer, response_model=ResCreateLyr)
async def create_layer(req:ReqCreateLyr):
    response = await http_handling(
        req,
        ReqCreateLyr,
        ResCreateLyr,
        fetch_or_create_lyr,
    )
    return response

@app.post(CONF.save_producer_layer, response_model=ResAcknowlg)
async def save_producer_layer(req:ReqSavePrdcerLyer):
    response = await http_handling(
        req,
        ReqSavePrdcerLyer,
        ResAcknowlg,
        create_save_prdcer_lyr,
    )
    return response



@app.post(CONF.user_layers, response_model=UserLayersResponse)
async def user_layers(req: UserIdRequest):
    response = await http_handling(
        req,
        UserIdRequest,
        UserLayersResponse,
        fetch_prdcer_lyrs
    )
    return response



@app.post(CONF.prdcer_lyr_map_data, response_model=ResPrdcerLyrMapData)
async def prdcer_lyr_map_data(req: ReqPrdcerLyrMapData):
    response = await http_handling(
        req,
        ReqPrdcerLyrMapData,
        ResPrdcerLyrMapData,
        fetch_prdcer_lyr_map_data
    )
    return response



@app.post(CONF.save_producer_catalog, response_model=ResSavePrdcerCtlg)
async def save_producer_catalog(req: ReqSavePrdcerCtlg):
    response = await http_handling(
        req,
        ReqSavePrdcerCtlg,
        ResSavePrdcerCtlg,
        create_save_prdcer_ctlg,
    )
    return response





@app.post(CONF.user_catalogs, response_model=UserCatalogsResponse)
async def user_catalogs(req: UserIdRequest):
    response = await http_handling(
        req,
        UserIdRequest,
        UserCatalogsResponse,
        fetch_prdcer_ctlgs
    )
    return response



















