import uuid
from typing import Awaitable
from typing import Optional, Callable, Any, Type, TypeVar
from auth import decode_access_token
from fastapi import Depends
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, ValidationError

from fastapi import HTTPException, status
from pydantic import ValidationError
import uuid
import logging
from typing import Optional, Type, Callable, Awaitable, Any, TypeVar
from fastapi.responses import JSONResponse
logger = logging.getLogger(__name__)
from all_types.myapi_dtypes import ReqApplyZoneLayers, ResApplyZoneLayers
from all_types.myapi_dtypes import (
    ReqLocation,
    ReqCatalogId,
    ResAllCards,
    ResAcknowlg,
    ResTypeMapData,
    ResCountryCityData,
    ResNearbyCategories,
    ResOldNearbyCategories,
    ReqCreateLyr,
    ResCreateLyr,
    ReqSavePrdcerLyer,
    ReqPrdcerLyrMapData,
    ResPrdcerLyrMapData,
    ReqCreateUserProfile,
    ResCreateUserProfile,
    RequestModel,
    ResToken,
    ReqUserLogin,
    ReqUserProfile,
    ResUserProfile,
    ReqUserProfileWithToken
)
from all_types.myapi_dtypes import (
    ReqUserId,
    ResUserLayers,
    ReqSavePrdcerCtlg,
    ResSavePrdcerCtlg,
)
from all_types.myapi_dtypes import ResUserCatalogs, ReqFetchCtlgLyrs, ResCtlgLyrs
from config_factory import get_conf
from data_fetcher import (
    get_boxmap_catlog_data,
    fetch_catlog_collection,
    nearby_boxmap,
    fetch_layer_collection,
    old_fetch_nearby_categories,
    fetch_country_city_category_map_data,
    save_lyr,
    fetch_user_lyrs,
    fetch_lyr_map_data,
    create_save_prdcer_ctlg,
    fetch_prdcer_ctlgs,
    fetch_ctlg_lyrs,
    apply_zone_layers,
    create_user_profile,
    login_user,
    get_user_profile
)
from storage import fetch_country_city_data, fetch_nearby_categories
T = TypeVar('T', bound=BaseModel)
U = TypeVar('U', bound=BaseModel)

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
        custom_function: Callable[[Any], Awaitable[Any]],
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
        req: Optional[T],
        input_type: Optional[Type[T]],
        output_type: Type[U],
        custom_function: Optional[Callable[..., Awaitable[Any]]],
):
    try:
        output = ""
        if req is not None:
            # Verify access token if it exists
            if hasattr(req, 'access_token'):
                try:
                    payload = decode_access_token(req.access_token)
                    token_user_id = payload.get("sub")
                    # Check if the token user_id matches the requested user_id
                    if hasattr(req.request_body, 'user_id') and token_user_id != req.request_body.user_id:
                        raise HTTPException(
                            status_code=status.HTTP_403_FORBIDDEN,
                            detail="You can only access your own profile",
                        )
                except HTTPException:
                    raise
                except Exception as e:
                    logger.error(f"Token validation error: {str(e)}")
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Invalid access token",
                        headers={"WWW-Authenticate": "Bearer"},
                    )

            req = req.request_body
            try:
                input_type.model_validate(req)
            except ValidationError as e:
                logger.error(f"Request validation error: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid request body: {str(e)}"
                ) from e

        if custom_function is not None:
            try:
                output = await custom_function(req=req)
            except Exception as e:
                logger.error(f"Custom function error: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="An error occurred while processing your request"
                ) from e

        request_id = "req-" + str(uuid.uuid4())
        
        try:
            res_body = output_type(
                message="Request received",
                request_id=request_id,
                data=output,
            )
        except ValidationError as e:
            logger.error(f"Response validation error: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="An error occurred while constructing the response"
            ) from e

        return res_body

    except HTTPException as http_exc:
        # Log the exception and return it directly
        logger.error(f"HTTP exception: {http_exc.detail}")
        return JSONResponse(
            status_code=http_exc.status_code,
            content={"detail": http_exc.detail}
        )
    except Exception as e:
        # Catch any other unexpected exceptions
        logger.critical(f"Unexpected error in http_handling: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "An unexpected error occurred"}
        )


@app.post(CONF.http_catlog_data, response_model=ResTypeMapData)
async def catlog_data(catlog_req: RequestModel[ReqCatalogId]):
    response = await http_handling(
        catlog_req,
        ReqCatalogId,
        ResTypeMapData,
        get_boxmap_catlog_data,
    )
    return response


@app.post(CONF.http_single_nearby, response_model=ResTypeMapData)
async def single_nearby(req: RequestModel[ReqLocation]):
    response = await http_handling(
        req,
        ReqLocation,
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


@app.get(CONF.catlog_collection, response_model=ResAllCards)
async def catlog_collection():
    response = await http_handling(
        None,
        None,
        ResAllCards,
        fetch_catlog_collection,
    )
    return response


@app.get(CONF.layer_collection, response_model=ResAllCards)
async def layer_collection():
    response = await http_handling(
        None,
        None,
        ResAllCards,
        fetch_layer_collection,
    )
    return response


@app.get(CONF.country_city, response_model=ResCountryCityData)
async def country_city():
    response = await http_handling(
        None,
        None,
        ResCountryCityData,
        fetch_country_city_data,
    )
    return response


@app.get(CONF.nearby_categories, response_model=ResNearbyCategories)
async def nearby_categories():
    response = await http_handling(
        None,
        None,
        ResNearbyCategories,
        fetch_nearby_categories,
    )
    return response


@app.get(CONF.old_nearby_categories, response_model=ResOldNearbyCategories)
async def old_nearby_categories():
    response = await http_handling(
        None,
        None,
        ResOldNearbyCategories,
        old_fetch_nearby_categories,
    )
    return response


@app.post(CONF.create_layer, response_model=ResCreateLyr)
async def create_layer(req: RequestModel[ReqCreateLyr]):
    response = await http_handling(
        req,
        ReqCreateLyr,
        ResCreateLyr,
        fetch_country_city_category_map_data,
    )
    return response


@app.post(CONF.save_producer_layer, response_model=ResAcknowlg)
async def save_producer_layer(req: RequestModel[ReqSavePrdcerLyer]):
    response = await http_handling(
        req,
        ReqSavePrdcerLyer,
        ResAcknowlg,
        save_lyr,
    )
    return response


@app.post(CONF.user_layers, response_model=ResUserLayers)
async def user_layers(req: RequestModel[ReqUserId]):
    response = await http_handling(
        req, ReqUserId, ResUserLayers, fetch_user_lyrs
    )
    return response


@app.post(CONF.prdcer_lyr_map_data, response_model=ResPrdcerLyrMapData)
async def prdcer_lyr_map_data(req: RequestModel[ReqPrdcerLyrMapData]):
    response = await http_handling(
        req, ReqPrdcerLyrMapData, ResPrdcerLyrMapData, fetch_lyr_map_data
    )
    return response


@app.post(CONF.save_producer_catalog, response_model=ResSavePrdcerCtlg)
async def save_producer_catalog(req: RequestModel[ReqSavePrdcerCtlg]):
    response = await http_handling(
        req,
        ReqSavePrdcerCtlg,
        ResSavePrdcerCtlg,
        create_save_prdcer_ctlg,
    )
    return response


@app.post(CONF.user_catalogs, response_model=ResUserCatalogs)
async def user_catalogs(req: RequestModel[ReqUserId]):
    response = await http_handling(
        req, ReqUserId, ResUserCatalogs, fetch_prdcer_ctlgs
    )
    return response


@app.post(CONF.fetch_ctlg_lyrs, response_model=ResCtlgLyrs)
async def fetch_catalog_layers(req: RequestModel[ReqFetchCtlgLyrs]):
    response = await http_handling(req, ReqFetchCtlgLyrs, ResCtlgLyrs, fetch_ctlg_lyrs)
    return response


@app.post(CONF.apply_zone_layers, response_model=ResApplyZoneLayers)
async def apply_zone_layers_endpoint(req: RequestModel[ReqApplyZoneLayers]):
    response = await http_handling(
        req, ReqApplyZoneLayers, ResApplyZoneLayers, apply_zone_layers
    )
    return response


@app.post(CONF.create_user_profile, response_model=ResCreateUserProfile)
async def create_user_profile_endpoint(req: RequestModel[ReqCreateUserProfile]):
    response = await http_handling(
        req,
        ReqCreateUserProfile,
        ResCreateUserProfile,
        create_user_profile
    )
    return response


@app.post(CONF.login, response_model=ResToken)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    req = ReqUserLogin(username=form_data.username, password=form_data.password)
    wrapped_req = RequestModel(message="Login request", request_info={},
                               request_body=req)  # test that we are not losing info
    response = await http_handling(
        wrapped_req,
        ReqUserLogin,
        ResToken,
        login_user
    )
    return response


@app.post(CONF.user_profile, response_model=ResUserProfile)
async def get_user_profile_endpoint(req: ReqUserProfileWithToken):
    response = await http_handling(
        req,
        ReqUserProfile,
        ResUserProfile,
        get_user_profile
    )
    return response
