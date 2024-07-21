import math
import uuid
import numpy as np
import logging
from fastapi import HTTPException
from fastapi import status
from logging_wrapper import log_and_validate
from all_types.myapi_dtypes import ReqLocation, ReqCatalogId, Feature, Geometry
from all_types.myapi_dtypes import (
    ReqSavePrdcerLyer,
    LayerInfo,
    ReqUserId,
    PrdcerLyrMapData,
    ReqSavePrdcerCtlg,
    UserCatalogInfo,
    ReqFetchCtlgLyrs,
    ReqApplyZoneLayers,
    ReqPrdcerLyrMapData,
    ReqCreateUserProfile,
    MapData,
    ReqUserLogin,
    ReqUserProfile,
    ReqCreateLyr,
    ResCreateLyr,
)
from auth import authenticate_user, create_access_token
from auth import get_password_hash
from google_api_connector import (
    fetch_from_google_maps_api,
    old_fetch_from_google_maps_api,
)
from mapbox_connector import MapBoxConnector
from storage import generate_user_id
from storage import (
    get_data_from_storage,
    store_data,
    get_dataset_from_storage,
    search_metastore_for_string,
    fetch_dataset_id,
    load_dataset,
    fetch_layer_owner,
    load_user_profile,
    update_dataset_layer_matching,
    update_user_layer_matching,
    fetch_user_catalogs,
    update_user_profile,
    load_dataset_layer_matching,
    fetch_user_layers,
    load_store_catalogs,
    save_dataset,
    update_metastore,
    fetch_country_city_data,
    convert_to_serializable,
)
from storage import is_username_or_email_taken, add_user_to_info, generate_layer_id

from fastapi import HTTPException
from typing import List, Dict, Any
from logging_wrapper import apply_decorator_to_module, preserve_validate_decorator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def fetch_ggl_nearby(location_req: ReqLocation):
    """
    This function fetches nearby points of interest (POIs) based on a given location request.
    It first tries to retrieve the data from storage. If the data isn't found in storage,
    it fetches the data from the Google Maps API after a short delay. The fetched data is
    then stored for future use before being returned.
    """
    bknd_dataset_id = None
    dataset = None
    next_page_token = None
    # Try to get data from storage
    dataset = await get_data_from_storage(location_req)

    if not dataset:
        # If data is not in storage, fetch from Google Maps API
        # dataset, next_page_token = await fetch_from_google_maps_api(location_req)
        dataset, next_page_token = await old_fetch_from_google_maps_api(location_req)

        if dataset is not None:
            # Store the fetched data in storage
            await store_data(location_req, dataset)
            # Generate a new backend dataset ID
            bknd_dataset_id = str(uuid.uuid4())
            # Save the new dataset
            save_dataset(bknd_dataset_id, dataset)

    return dataset, bknd_dataset_id, next_page_token


async def get_catalogue_dataset(catalogue_dataset_id: str):
    """
    Retrieves a specific catalogue dataset from storage based on the provided ID.
    If the dataset is not found, it returns an empty dictionary. This function
    acts as a wrapper around the storage retrieval mechanism.
    """

    data = await get_dataset_from_storage(catalogue_dataset_id)
    return data


async def fetch_catlog_collection(**_):
    """
    Generates and returns a collection of catalog metadata. This function creates
    a list of predefined catalog entries and then adds 20 more dummy entries.
    Each entry contains information such as ID, name, description, thumbnail URL,
    and access permissions. This is likely used for testing or as placeholder data.
    """

    metadata = [
        {
            "id": "2",
            "name": "Saudi Arabia - Real Estate Transactions",
            "description": "Database of real-estate transactions in Saudi Arabia",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/real_estate_ksa.png",
            "catalog_link": "https://example.com/catalog2.jpg",
            "records_number": 20,
            "can_access": True,
        },
        {
            "id": "55",
            "name": "Saudi Arabia - gas stations poi data",
            "description": "Database of all Saudi Arabia gas stations Points of Interests",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/SAUgasStations.PNG",
            "catalog_link": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/SAUgasStations.PNG",
            "records_number": 8517,
            "can_access": False,
        },
        {
            "id": "55",
            "name": "Saudi Arabia - Restaurants, Cafes and Bakeries",
            "description": "Focusing on the restaurants, cafes and bakeries in KSA",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/sau_bak_res.PNG",
            "catalog_link": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/sau_bak_res.PNG",
            "records_number": 132383,
            "can_access": False,
        },
    ]

    # Add 20 more dummy entries
    for i in range(3, 4):
        metadata.append(
            {
                "id": str(i),
                "name": f"Saudi Arabia - Sample Data {i}",
                "description": f"Sample description for dataset {i}",
                "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/sample_image.png",
                "catalog_link": "https://example.com/sample_image.jpg",
                "records_number": i * 100,
                "can_access": True,
            }
        )

    return metadata


async def fetch_layer_collection(**_):
    """
    Similar to fetch_catlog_collection, this function returns a collection of layer
    metadata. It provides a smaller, fixed set of layer entries. Each entry includes
    details like ID, name, description, and access permissions.
    """

    metadata = [
        {
            "id": "2",
            "name": "Saudi Arabia - Real Estate Transactions",
            "description": "Database of real-estate transactions in Saudi Arabia",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/real_estate_ksa.png",
            "catalog_link": "https://example.com/catalog2.jpg",
            "records_number": 20,
            "can_access": False,
        },
        {
            "id": "3",
            "name": "Saudi Arabia - 3",
            "description": "Database of all Saudi Arabia gas stations Points of Interests",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/SAUgasStations.PNG",
            "catalog_link": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/SAUgasStations.PNG",
            "records_number": 8517,
            "can_access": False,
        },
    ]

    return metadata


async def get_boxmap_catlog_data(req: ReqCatalogId):
    """
    This function retrieves catalog data for a specific catalog ID and transforms
    it into a format suitable for box mapping. It uses the get_catalogue_dataset
    function to fetch the raw data, then applies a transformation using the
    MapBoxConnector to convert it into the required format.
    """

    response_data = await get_catalogue_dataset(req.catalogue_dataset_id)
    trans_data = await MapBoxConnector.ggl_to_boxmap(response_data)
    return trans_data


async def nearby_boxmap(req):
    """
    Fetches nearby data based on the provided request and transforms it into
    a format suitable for box mapping. It uses the fetch_nearby function to get
    the raw data, then applies a transformation using the MapBoxConnector.
    """

    response_data, _, _ = await fetch_ggl_nearby(req)
    trans_data = await MapBoxConnector.new_ggl_to_boxmap(response_data)
    return trans_data


async def old_fetch_nearby_categories(**_):
    """
    Returns an older, simplified version of nearby categories. Unlike the newer
    version, this function provides a flat list of category names, primarily
    focused on food and drink establishments.
    """

    categories = [
        "american_restaurant",
        "bakery",
        "bar",
        "barbecue_restaurant",
        "brazilian_restaurant",
        "breakfast_restaurant",
        "brunch_restaurant",
        "cafe",
        "chinese_restaurant",
        "coffee_shop",
        "fast_food_restaurant",
        "french_restaurant",
        "greek_restaurant",
        "hamburger_restaurant",
        "ice_cream_shop",
        "indian_restaurant",
        "indonesian_restaurant",
        "italian_restaurant",
        "japanese_restaurant",
        "korean_restaurant",
        "lebanese_restaurant",
        "meal_delivery",
        "meal_takeaway",
        "mediterranean_restaurant",
        "mexican_restaurant",
        "middle_eastern_restaurant",
        "pizza_restaurant",
        "ramen_restaurant",
        "restaurant",
        "sandwich_shop",
        "seafood_restaurant",
        "spanish_restaurant",
        "steak_house",
        "sushi_restaurant",
        "thai_restaurant",
        "turkish_restaurant",
        "vegan_restaurant",
        "vegetarian_restaurant",
        "vietnamese_restaurant",
    ]
    return categories


async def fetch_country_city_category_map_data(req: ReqCreateLyr) -> ResCreateLyr:
    """
    This function attempts to fetch an existing layer based on the provided
    request parameters. If the layer exists, it loads the data, transforms it,
    and returns it. If the layer doesn't exist, it creates a new layer by
    fetching data from Google Maps API.
    """
    next_page_token = None
    dataset_category = req.dataset_category
    dataset_country = req.dataset_country
    dataset_city = req.dataset_city
    page_token = req.page_token
    ccc_filename = f"{dataset_category}_{dataset_country}_{dataset_city}.json"
    existing_layer = await search_metastore_for_string(ccc_filename)

    # first check if there is a dataset already
    # if yes load it
    # check if page_token
    # if page_token is None:
    # return      trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)
    #     trans_dataset["bknd_dataset_id"] = bknd_dataset_id
    #     trans_dataset["records_count"] = len(trans_dataset["features"])
    #     trans_dataset["prdcer_lyr_id"] = generate_layer_id()
    # if page token is not None:

    if existing_layer:
        bknd_dataset_id = existing_layer["bknd_dataset_id"]
        dataset = load_dataset(bknd_dataset_id)

    if not existing_layer or page_token is not None:
        existing_dataset = []
        # Fetch country and city data
        country_city_data = await fetch_country_city_data()

        # Find the city data
        city_data = None
        for country, cities in country_city_data.items():
            if country == dataset_country:
                for city in cities:
                    if city["name"] == dataset_city:
                        city_data = city
                        break
                if city_data:
                    break

        if not city_data:
            raise HTTPException(
                status_code=404, detail="City not found in the specified country"
            )

        # Create new dataset request
        new_dataset_req = ReqLocation(
            lat=city_data["lat"],
            lng=city_data["lng"],
            radius=50000,
            type=dataset_category,
            page_token=page_token,
        )

        # Fetch data from Google Maps API
        dataset, bknd_dataset_id, next_page_token = await fetch_ggl_nearby(
            new_dataset_req
        )
        # Update metastore
        update_metastore(ccc_filename, bknd_dataset_id)

        # Append new data to existing dataset
        existing_dataset.extend(dataset)
        # Save updated dataset
        save_dataset(bknd_dataset_id, existing_dataset)

    trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)
    trans_dataset["bknd_dataset_id"] = bknd_dataset_id
    trans_dataset["records_count"] = len(trans_dataset["features"])
    trans_dataset["prdcer_lyr_id"] = generate_layer_id()
    trans_dataset["next_page_token"] = next_page_token
    return trans_dataset


async def save_lyr(req: ReqSavePrdcerLyer) -> str:
    try:
        user_data = load_user_profile(req.user_id)
    except FileNotFoundError:
        logger.error(f"User profile not found for user_id: {req.user_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User profile not found"
        )

    try:
        # Add the new layer to user profile
        user_data["prdcer"]["prdcer_lyrs"][req.prdcer_lyr_id] = req.dict(
            exclude={"user_id"}
        )

        # Save updated user data
        update_user_profile(req.user_id, user_data)
        update_dataset_layer_matching(req.prdcer_lyr_id, req.bknd_dataset_id)
        update_user_layer_matching(req.prdcer_lyr_id, req.user_id)
    except KeyError:
        logger.error(f"Invalid user data structure for user_id: {req.user_id}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid user data structure",
        )

    return "Producer layer created successfully"


@preserve_validate_decorator
@log_and_validate(logger, validate_output=True, output_model=List[LayerInfo])
async def fetch_user_lyrs(req: ReqUserId) -> List[LayerInfo]:
    """
    Retrieves all producer layers associated with a specific user. It reads the
    user's data file and the dataset-layer matching file to compile a list of
    all layers owned by the user, including metadata like layer name, color,
    and record count.
    """
    try:
        # Load dataset_layer_matching.json
        dataset_layer_matching = load_dataset_layer_matching()
    except FileNotFoundError:
        logger.error("Dataset-layer matching file not found")
        raise HTTPException(
            status_code=500, detail="Dataset-layer matching data not available"
        )

    try:
        user_layers = fetch_user_layers(req.user_id)
    except FileNotFoundError:
        logger.error(f"User layers not found for user_id: {req.user_id}")
        raise HTTPException(status_code=404, detail="User layers not found")

    user_layers_metadata = []
    for lyr_id, lyr_data in user_layers.items():
        try:
            dataset_id, dataset_info = fetch_dataset_id(lyr_id, dataset_layer_matching)
            records_count = dataset_info["records_count"]

            user_layers_metadata.append(
                LayerInfo(
                    prdcer_lyr_id=lyr_id,
                    prdcer_layer_name=lyr_data["prdcer_layer_name"],
                    points_color=lyr_data["points_color"],
                    layer_legend=lyr_data["layer_legend"],
                    layer_description=lyr_data["layer_description"],
                    records_count=records_count,
                    is_zone_lyr="false",  # Default to "false" as string
                )
            )
        except KeyError as e:
            logger.error(f"Missing key in layer data: {str(e)}")
            # Continue to next layer instead of failing the entire request
            continue

    if not user_layers_metadata:
        raise HTTPException(
            status_code=404, detail="No valid layers found for the user"
        )

    return user_layers_metadata


async def fetch_lyr_map_data(req: ReqPrdcerLyrMapData) -> PrdcerLyrMapData:
    """
    Fetches detailed map data for a specific producer layer.
    """
    try:
        layer_owner_id = fetch_layer_owner(req.prdcer_lyr_id)
        layer_owner_data = load_user_profile(layer_owner_id)

        try:
            layer_metadata = layer_owner_data["prdcer"]["prdcer_lyrs"][
                req.prdcer_lyr_id
            ]
        except KeyError:
            raise HTTPException(
                status_code=404, detail="Producer layer not found for this user"
            )

        dataset_id, dataset_info = fetch_dataset_id(req.prdcer_lyr_id)
        dataset = load_dataset(dataset_id)
        trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)

        return PrdcerLyrMapData(
            type="FeatureCollection",
            features=trans_dataset["features"],
            prdcer_layer_name=layer_metadata["prdcer_layer_name"],
            prdcer_lyr_id=req.prdcer_lyr_id,
            bknd_dataset_id=dataset_id,
            points_color=layer_metadata["points_color"],
            layer_legend=layer_metadata["layer_legend"],
            layer_description=layer_metadata["layer_description"],
            records_count=dataset_info["records_count"],
            is_zone_lyr="false",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


async def create_save_prdcer_ctlg(req: ReqSavePrdcerCtlg) -> str:
    """
    Creates and saves a new producer catalog.
    """
    try:
        user_data = load_user_profile(req.user_id)

        new_ctlg_id = str(uuid.uuid4())
        new_catalog = {
            "prdcer_ctlg_name": req.prdcer_ctlg_name,
            "prdcer_ctlg_id": new_ctlg_id,
            "subscription_price": req.subscription_price,
            "ctlg_description": req.ctlg_description,
            "total_records": req.total_records,
            "lyrs": req.lyrs,
            "thumbnail_url": req.thumbnail_url,
            "ctlg_owner_user_id": req.user_id,
        }
        user_data["prdcer"]["prdcer_ctlgs"][new_ctlg_id] = new_catalog

        serializable_user_data = convert_to_serializable(user_data)
        update_user_profile(req.user_id, serializable_user_data)

        return new_ctlg_id
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while creating catalog: {str(e)}",
        )


async def fetch_prdcer_ctlgs(req: ReqUserId) -> List[UserCatalogInfo]:
    """
    Retrieves all producer catalogs associated with a specific user.
    """
    try:
        user_catalogs = fetch_user_catalogs(req.user_id)
        return [
            UserCatalogInfo(
                prdcer_ctlg_id=ctlg_id,
                prdcer_ctlg_name=ctlg_data["prdcer_ctlg_name"],
                ctlg_description=ctlg_data["ctlg_description"],
                thumbnail_url=ctlg_data.get("thumbnail_url", ""),
                subscription_price=ctlg_data["subscription_price"],
                total_records=ctlg_data["total_records"],
                lyrs=ctlg_data["lyrs"],
                ctlg_owner_user_id=ctlg_data["ctlg_owner_user_id"],
            )
            for ctlg_id, ctlg_data in user_catalogs.items()
        ]
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while fetching catalogs: {str(e)}",
        )


async def fetch_ctlg_lyrs(req: ReqFetchCtlgLyrs) -> List[PrdcerLyrMapData]:
    """
    Fetches all layers associated with a specific catalog.
    """
    try:
        user_data = load_user_profile(req.user_id)
        ctlg = (
            user_data.get("prdcer", {})
            .get("prdcer_ctlgs", {})
            .get(req.prdcer_ctlg_id, {})
        )

        if not ctlg:
            store_ctlgs = load_store_catalogs()
            ctlg = next(
                (
                    ctlg_info
                    for ctlg_key, ctlg_info in store_ctlgs.items()
                    if ctlg_key == req.prdcer_ctlg_id
                ),
                {},
            )

        if not ctlg:
            raise HTTPException(status_code=404, detail="Catalog not found")

        dataset_layer_matching = load_dataset_layer_matching()
        ctlg_owner_data = load_user_profile(ctlg["ctlg_owner_user_id"])

        ctlg_lyrs_map_data = []
        for lyr_id in ctlg["lyrs"]:
            dataset_id, dataset_info = fetch_dataset_id(lyr_id, dataset_layer_matching)
            dataset = load_dataset(dataset_id)
            trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)

            lyr_metadata = (
                ctlg_owner_data.get("prdcer", {}).get("prdcer_lyrs", {}).get(lyr_id, {})
            )

            ctlg_lyrs_map_data.append(
                PrdcerLyrMapData(
                    type="FeatureCollection",
                    features=trans_dataset["features"],
                    prdcer_layer_name=lyr_metadata.get(
                        "prdcer_layer_name", f"Layer {lyr_id}"
                    ),
                    prdcer_lyr_id=lyr_id,
                    bknd_dataset_id=dataset_id,
                    points_color=lyr_metadata.get("points_color", "red"),
                    layer_legend=lyr_metadata.get("layer_legend", ""),
                    layer_description=lyr_metadata.get("layer_description", ""),
                    records_count=len(trans_dataset["features"]),
                    is_zone_lyr="false",
                )
            )

        return ctlg_lyrs_map_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


async def apply_zone_layers(req: ReqApplyZoneLayers) -> List[PrdcerLyrMapData]:
    """
    Applies zone layer transformations to a set of layers.
    """
    try:
        non_zone_layers = req.lyrs.copy()
        zone_layers = []
        for layer in req.lyrs_as_zone:
            zone_lyr_id = list(layer.keys())[0]
            zone_layers.append(zone_lyr_id)
            non_zone_layers.remove(zone_lyr_id)

        dataset_layer_matching = load_dataset_layer_matching()

        non_zone_data = []
        for lyr_id in non_zone_layers:
            dataset_id, _ = fetch_dataset_id(lyr_id, dataset_layer_matching)
            if dataset_id:
                dataset = load_dataset(dataset_id)
                lyr_data = await MapBoxConnector.new_ggl_to_boxmap(dataset)
                non_zone_data.extend(lyr_data["features"])

        zone_data = {}
        for lyr_id in zone_layers:
            dataset_id, _ = fetch_dataset_id(lyr_id, dataset_layer_matching)
            if dataset_id:
                dataset = load_dataset(dataset_id)
                lyr_data = await MapBoxConnector.new_ggl_to_boxmap(dataset)
                zone_data[lyr_id] = lyr_data

        transformed_layers = []
        for layer in req.lyrs_as_zone:
            zone_lyr_id = list(layer.keys())[0]
            zone_property_key = list(layer.values())[0]
            zone_transformed = apply_zone_transformation(
                zone_data[zone_lyr_id], non_zone_data, zone_property_key, zone_lyr_id
            )
            transformed_layers.extend(zone_transformed)

        return transformed_layers
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


def apply_zone_transformation(
    zone_layer_data: Dict[str, Any],
    non_zone_points: List[Dict[str, Any]],
    zone_property: str,
    zone_lyr_id: str,
) -> List[PrdcerLyrMapData]:
    """
    This function applies zone transformations to a set of points.
    """
    try:
        zone_property = zone_property.split("features.properties.")[-1]
        property_values = [
            feature["properties"].get(zone_property, 9191919191)
            for feature in zone_layer_data["features"]
        ]
        arr = np.array(property_values)
        avg = np.mean(arr[arr != 9191919191.0])
        new_arr = np.where(arr == 9191919191.0, avg, arr)
        property_values = new_arr.tolist()
        thresholds = calculate_thresholds(property_values)

        new_layers = [
            PrdcerLyrMapData(
                type="FeatureCollection",
                features=[],
                prdcer_layer_name=f"{zone_layer_data.get('prdcer_layer_name', 'Layer')} ({category})",
                prdcer_lyr_id=f"zy{zone_lyr_id}_applied_{i + 1}",
                points_color=color,
                layer_legend=f"{zone_layer_data.get('layer_legend', 'Layer')} {category} {zone_property}",
                records_count=0,
                is_zone_lyr="False",
                bknd_dataset_id=zone_layer_data.get("bknd_dataset_id", ""),
                layer_description=zone_layer_data.get("layer_description", ""),
            )
            for i, (category, color) in enumerate(
                [
                    ("low", "grey"),
                    ("medium", "cyan"),
                    ("high", "red"),
                    ("non-zone-overlap", "blue"),
                ]
            )
        ]

        for point in non_zone_points:
            point_coords = point["geometry"]["coordinates"]
            for zone_feature in zone_layer_data["features"]:
                zone_point = zone_feature["geometry"]["coordinates"]
                if calculate_distance_km(point_coords, zone_point) <= 2:
                    value = zone_feature["properties"].get(zone_property, 0)
                    if value <= thresholds[0]:
                        new_layers[0].features.append(create_feature(point))
                    elif value <= thresholds[1]:
                        new_layers[1].features.append(create_feature(point))
                    else:
                        new_layers[2].features.append(create_feature(point))
                    break
                else:
                    new_layers[3].features.append(create_feature(point))

        for layer in new_layers:
            layer.records_count = len(layer.features)

        return new_layers
    except Exception as e:
        raise ValueError(f"Error in apply_zone_transformation: {str(e)}")


def calculate_thresholds(values: List[float]) -> List[float]:
    """
    Calculates threshold values to divide a set of values into three categories.
    """
    try:
        sorted_values = sorted(values)
        n = len(sorted_values)
        return [sorted_values[n // 3], sorted_values[2 * n // 3]]
    except Exception as e:
        raise ValueError(f"Error in calculate_thresholds: {str(e)}")


def calculate_distance_km(point1: List[float], point2: List[float]) -> float:
    """
    Calculates the distance between two points in kilometers using the Haversine formula.
    """
    try:
        R = 6371
        lon1, lat1 = math.radians(point1[0]), math.radians(point1[1])
        lon2, lat2 = math.radians(point2[0]), math.radians(point2[1])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        return distance
    except Exception as e:
        raise ValueError(f"Error in calculate_distance_km: {str(e)}")


async def login_user(req: ReqUserLogin) -> Dict[str, str]:
    try:
        user = authenticate_user(req.username, req.password)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token = create_access_token(data={"sub": user["user_id"]})
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user_id": user["user_id"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during login: {str(e)}",
        )



async def create_user_profile(req: ReqCreateUserProfile) -> Dict[str, str]:
    try:
        if is_username_or_email_taken(req.username, req.email):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username or email already taken",
            )
        user_id = generate_user_id()
        hashed_password = get_password_hash(req.password)

        user_data = {
            "user_id": user_id,
            "username": req.username,
            "email": req.email,
            "prdcer": {"prdcer_lyrs": {}, "prdcer_ctlgs": {}},
        }

        update_user_profile(user_id, user_data)
        add_user_to_info(user_id, req.username, req.email, hashed_password)

        return {"user_id": user_id, "message": "User profile created successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating user profile: {str(e)}",
        )


async def get_user_profile(req: ReqUserProfile) -> Dict[str, Any]:
    try:
        user_data = load_user_profile(req.user_id)
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )

        return {
            "user_id": user_data["user_id"],
            "username": user_data["username"],
            "email": user_data["email"],
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching user profile: {str(e)}",
        )


def create_feature(point: Dict[str, Any]) -> Feature:
    """
    Converts a point dictionary into a Feature object. This function is used
    to ensure that all points are in the correct format for geospatial operations.
    """
    try:
        return Feature(
            type=point["type"],
            properties=point["properties"],
            geometry=Geometry(
                type="Point", coordinates=point["geometry"]["coordinates"]
            ),
        )
    except KeyError as e:
        raise ValueError(f"Invalid point data: missing key {str(e)}")
    except Exception as e:
        raise ValueError(f"Error creating feature: {str(e)}")


# Apply the decorator to all functions in this module
apply_decorator_to_module(logger)(__name__)

