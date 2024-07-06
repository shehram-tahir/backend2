from all_types.google_dtypes import GglResponse
from all_types.myapi_dtypes import LocationReq, CatlogId
from all_types.myapi_dtypes import ReqApplyZoneLayers, PrdcerLyrMapData
from all_types.myapi_dtypes import (
    CountryCityData,
    ReqSavePrdcerLyer,
    UserLayerInfo,
    UserIdRequest,
    PrdcerLyrMapData,
    ReqSavePrdcerCtlg,
    UserCatalogInfo,
    ReqFetchCtlgLyrs,
    ReqApplyZoneLayers,
    Feature,
    Geometry,
    ReqPrdcerLyrMapData
)
from google_api_connector import fetch_from_google_maps_api
from mapbox_connector import MapBoxConnector
from storage import (
    get_data_from_storage,
    store_data,
    get_dataset_from_storage,
    search_metastore_for_string,
)
import math
import json
import os

from typing import List, Dict
import asyncio
import os
import json
from fastapi import HTTPException
import uuid
import json
import os
import numpy as np


USERS_PATH = "Backend/users"
STORE_CATALOGS_PATH = "Backend/catalogs/store_catalogs.json"
DATASET_LAYER_MATCHING_PATH = "Backend/dataset_layer_matching.json"
DATASETS_PATH = "Backend/datasets"
USER_LAYER_MATCHING_PATH = "Backend/user_layer_matching.json"

async def fetch_nearby(location_req: LocationReq):
    """
    This function fetches nearby points of interest (POIs) based on a given location request.
    It first tries to retrieve the data from storage. If the data isn't found in storage,
    it fetches the data from the Google Maps API after a short delay. The fetched data is
    then stored for future use before being returned.
    """
    # Try to get data from storage
    data = await get_data_from_storage(location_req)
    if not data:
        await asyncio.sleep(2)
        # If data is not in storage, fetch from Google Maps API
        data = await fetch_from_google_maps_api(location_req)
        # Store the fetched data in storage
        await store_data(location_req, data)
    return data


async def get_catalogue_dataset(catalogue_dataset_id: str):
    """
    Retrieves a specific catalogue dataset from storage based on the provided ID.
    If the dataset is not found, it returns an empty dictionary. This function
    acts as a wrapper around the storage retrieval mechanism.
    """

    data = await get_dataset_from_storage(catalogue_dataset_id)
    if not data:
        data = {}
    return data


async def fetch_catlog_collection(**kwargs):
    """
    Generates and returns a collection of catalog metadata. This function creates
    a list of predefined catalog entries and then adds 20 more dummy entries.
    Each entry contains information such as ID, name, description, thumbnail URL,
    and access permissions. This is likely used for testing or as placeholder data.
    """

    metadata = [
        {
            "id": "1",
            "name": "Saudi Arabia - gas stations poi data",
            "description": "Database of all Saudi Arabia gas stations Points of Interests",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/real_estate_ksa.png",
            "catalog_link": "https://example.com/catalog2.jpg",
            "records_number": 10,
            "can_access": True,
        },
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
            "id": "5218f0ef-c4db-4441-81e2-83ce413a9645",
            "name": "Saudi Arabia - gas stations poi data",
            "description": "Database of all Saudi Arabia gas stations Points of Interests",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/SAUgasStations.PNG",
            "catalog_link": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/SAUgasStations.PNG",
            "records_number": 8517,
            "can_access": False,
        },
        {
            "id": "3e5ee589-25e6-4cae-8aec-3ed3cdecef94",
            "name": "Saudi Arabia - Restaurants, Cafes and Bakeries",
            "description": "Focusing on the restaurants, cafes and bakeries in KSA",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/sau_bak_res.PNG",
            "catalog_link": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/sau_bak_res.PNG",
            "records_number": 132383,
            "can_access": True,
        },
        {
            "id": "c4eb5d56-4fcf-4095-8037-4c84894fd014",
            "name": "Saudi Arabia - Real Estate Transactions",
            "description": "Database of real-estate transactions in Saudi Arabia",
            "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/real_estate_ksa.png",
            "catalog_link": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/real_estate_ksa.png",
            "records_number": 179141,
            "can_access": False,
        },
    ]

    # Add 20 more dummy entries
    for i in range(3, 23):
        metadata.append(
            {
                "id": str(i),
                "name": f"Saudi Arabia - Sample Data {i}",
                "description": f"Sample description for dataset {i}",
                "thumbnail_url": "https://catalog-assets.s3.ap-northeast-1.amazonaws.com/sample_image.png",
                "catalog_link": "https://example.com/sample_image.jpg",
                "records_number": i * 100,
                "can_access": i % 2 == 0,
            }
        )

    return metadata


async def fetch_layer_collection(**kwargs):
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


async def get_boxmap_catlog_data(req: CatlogId):
    """
    This function retrieves catalog data for a specific catalog ID and transforms
    it into a format suitable for box mapping. It uses the get_catalogue_dataset
    function to fetch the raw data, then applies a transformation using the
    MapBoxConnector to convert it into the required format.
    """

    response_data: GglResponse = await get_catalogue_dataset(req.catalogue_dataset_id)
    trans_data = await MapBoxConnector.ggl_to_boxmap(response_data)
    return trans_data


async def nearby_boxmap(req):
    """
    Fetches nearby data based on the provided request and transforms it into
    a format suitable for box mapping. It uses the fetch_nearby function to get
    the raw data, then applies a transformation using the MapBoxConnector.
    """

    response_data = await fetch_nearby(req)
    trans_data = await MapBoxConnector.new_ggl_to_boxmap(response_data)
    return trans_data


async def fetch_country_city_data(**kwargs):
    """
    Returns a predefined set of country and city data. The data is structured
    as a dictionary where keys are country names and values are lists of cities
    with their coordinates and radii. This appears to be sample data for
    testing or demonstration purposes.
    """
    data = {
        "country1": [
            {"name": "city1", "lat": 37.7937, "lng": -122.3965, "radius": 1000},
            {"name": "city2", "lat": 37.7937, "lng": -122.3965, "radius": 1000},
            {"name": "city3", "lat": 37.7937, "lng": -122.3965, "radius": 1000},
        ],
        "country2": [
            {"name": "cityA", "lat": 37.7937, "lng": -122.3965, "radius": 1000},
            {"name": "cityB", "lat": 37.7937, "lng": -122.3965, "radius": 1000},
            {"name": "cityC", "lat": 37.7937, "lng": -122.3965, "radius": 1000},
        ],
    }

    return data


async def fetch_nearby_categories(**kwargs):
    """
    Provides a comprehensive list of nearby place categories, organized into
    broader categories. This function returns a large, predefined dictionary
    of categories and subcategories, covering various aspects of urban life
    such as automotive, culture, education, entertainment, and more.
    """
    categories = {
        "Automotive": [
            "car_dealer",
            "car_rental",
            "car_repair",
            "car_wash",
            "electric_vehicle_charging_station",
            "gas_station",
            "parking",
            "rest_stop",
        ],
        # "Business": ["farm"],
        "Culture": ["art_gallery", "museum", "performing_arts_theater"],
        "Education": [
            "library",
            "preschool",
            "primary_school",
            "school",
            "secondary_school",
            "university",
        ],
        "Entertainment and Recreation": [
            "amusement_center",
            "amusement_park",
            "aquarium",
            "banquet_hall",
            "bowling_alley",
            "casino",
            "community_center",
            "convention_center",
            "cultural_center",
            "dog_park",
            "event_venue",
            "hiking_area",
            "historical_landmark",
            "marina",
            "movie_rental",
            "movie_theater",
            "national_park",
            "night_club",
            "park",
            "tourist_attraction",
            "visitor_center",
            "wedding_venue",
            "zoo",
        ],
        "Finance": ["accounting", "atm", "bank"],
        "Food and Drink": [
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
        ],
        "Geographical Areas": [
            "administrative_area_level_1",
            "administrative_area_level_2",
            "country",
            "locality",
            "postal_code",
            "school_district",
        ],
        "Government": [
            "city_hall",
            "courthouse",
            "embassy",
            "fire_station",
            "local_government_office",
            "police",
            "post_office",
        ],
        "Health and Wellness": [
            "dental_clinic",
            "dentist",
            "doctor",
            "drugstore",
            "hospital",
            "medical_lab",
            "pharmacy",
            "physiotherapist",
            "spa",
        ],
        "Lodging": [
            "bed_and_breakfast",
            "campground",
            "camping_cabin",
            "cottage",
            "extended_stay_hotel",
            "farmstay",
            "guest_house",
            "hostel",
            "hotel",
            "lodging",
            "motel",
            "private_guest_room",
            "resort_hotel",
            "rv_park",
        ],
        "Places of Worship": ["church", "hindu_temple", "mosque", "synagogue"],
        "Services": [
            "barber_shop",
            "beauty_salon",
            "cemetery",
            "child_care_agency",
            "consultant",
            "courier_service",
            "electrician",
            "florist",
            "funeral_home",
            "hair_care",
            "hair_salon",
            "insurance_agency",
            "laundry",
            "lawyer",
            "locksmith",
            "moving_company",
            "painter",
            "plumber",
            "real_estate_agency",
            "roofing_contractor",
            "storage",
            "tailor",
            "telecommunications_service_provider",
            "travel_agency",
            "veterinary_care",
        ],
        "Shopping": [
            "auto_parts_store",
            "bicycle_store",
            "book_store",
            "cell_phone_store",
            "clothing_store",
            "convenience_store",
            "department_store",
            "discount_store",
            "electronics_store",
            "furniture_store",
            "gift_shop",
            "grocery_store",
            "hardware_store",
            "home_goods_store",
            "home_improvement_store",
            "jewelry_store",
            "liquor_store",
            "market",
            "pet_store",
            "shoe_store",
            "shopping_mall",
            "sporting_goods_store",
            "store",
            "supermarket",
            "wholesaler",
        ],
        "Sports": [
            "athletic_field",
            "fitness_center",
            "golf_course",
            "gym",
            "playground",
            "ski_resort",
            "sports_club",
            "sports_complex",
            "stadium",
            "swimming_pool",
        ],
        "Transportation": [
            "airport",
            "bus_station",
            "bus_stop",
            "ferry_terminal",
            "heliport",
            "light_rail_station",
            "park_and_ride",
            "subway_station",
            "taxi_stand",
            "train_station",
            "transit_depot",
            "transit_station",
            "truck_stop",
        ],
    }
    return categories


async def old_fetch_nearby_categories(**kwargs):
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


async def fetch_or_create_lyr(req):
    """
    This function attempts to fetch an existing layer based on the provided
    request parameters. If the layer exists, it loads the data, transforms it,
    and returns it. If the layer doesn't exist, it's supposed to create a new
    layer, but this part is not implemented in the provided code.
    """

    dataset_category = req.dataset_category
    dataset_country = req.dataset_country
    dataset_city = req.dataset_city
    layer_filename = f"{dataset_category}_{dataset_country}_{dataset_city}.json"
    existing_layer = await search_metastore_for_string(layer_filename)
    if existing_layer:
        bknd_dataset_id = existing_layer["bknd_dataset_id"]
        dataset = load_dataset(bknd_dataset_id)

        trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)
        trans_dataset["bknd_dataset_id"] = bknd_dataset_id
        trans_dataset["records_count"] = len(trans_dataset["features"])
        return trans_dataset
    else:
        # code to create layer
        pass


async def create_save_prdcer_lyr(req: ReqSavePrdcerLyer):
    """
    Creates and saves a new producer layer. This function updates both the user's
    data file and the dataset-layer matching file. It adds the new layer to the
    user's profile and updates the dataset-layer relationship. This ensures that
    the new layer is properly linked to both the user and the relevant dataset.
    """

    user_id = req.user_id
    user_file_path = os.path.join(USERS_PATH, f"user_{user_id}.json")

    # Load or create user data
    if os.path.exists(user_file_path):
        with open(user_file_path, "r") as f:
            user_data = json.load(f)
    else:
        user_data = {"prdcer": {"prdcer_lyrs": {}}}

    # Add the new producer layer
    user_data["prdcer"]["prdcer_lyrs"][req.prdcer_lyr_id] = req.dict(
        exclude={"user_id"}
    )

    # Save updated user data
    with open(user_file_path, "w") as f:
        json.dump(user_data, f, indent=2)

    # Update_dataset_layer_matching.json
    if os.path.exists(DATASET_LAYER_MATCHING_PATH):
        with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
            dataset_layer_matching = json.load(f)

    if req.bknd_dataset_id not in dataset_layer_matching:
        dataset_layer_matching[req.bknd_dataset_id] = []

    if req.prdcer_lyr_id not in dataset_layer_matching[req.bknd_dataset_id]:
        dataset_layer_matching[req.bknd_dataset_id].append(req.prdcer_lyr_id)

    with open(DATASET_LAYER_MATCHING_PATH, "w") as f:
        json.dump(dataset_layer_matching, f, indent=2)

    return "Producer layer created successfully"


async def fetch_prdcer_lyrs(req: UserIdRequest) -> list[UserLayerInfo]:
    """
    Retrieves all producer layers associated with a specific user. It reads the
    user's data file and the dataset-layer matching file to compile a list of
    all layers owned by the user, including metadata like layer name, color,
    and record count.
    """
    user_id = req.user_id

    user_file_path = os.path.join(USERS_PATH, f"user_{user_id}.json")

    if not os.path.exists(user_file_path):
        return []  # Return an empty list if the user file doesn't exist

    with open(user_file_path, "r") as f:
        user_data = json.load(f)

    # Load dataset_layer_matching.json
    with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
        dataset_layer_matching = json.load(f)

    user_layers = []
    for lyr_id, lyr_data in user_data.get("prdcer", {}).get("prdcer_lyrs", {}).items():
        # Find the corresponding dataset_id
        
        dataset_id = None
        for d_id, d_info in dataset_layer_matching.items():
            if lyr_id in d_info["prdcer_lyrs"]:
                dataset_id = d_id
                break

        if dataset_id:
            records_count = d_info["records_count"]
        else:
            records_count = 0  # Default if no matching dataset is found

        user_layers.append(
            UserLayerInfo(
                prdcer_lyr_id=lyr_id,
                prdcer_layer_name=lyr_data["prdcer_layer_name"],
                points_color=lyr_data["points_color"],
                layer_legend=lyr_data["layer_legend"],
                layer_description=lyr_data["layer_description"],
                records_count=records_count,
                is_zone_lyr="false",  # Default to "false" as string
            )
        )

    return user_layers

async def fetch_prdcer_lyr_map_data(req: ReqPrdcerLyrMapData):
    """
    Fetches detailed map data for a specific producer layer. This function
    retrieves the layer metadata from the user's profile, finds the associated
    dataset, loads and transforms the dataset, and combines it with the layer
    metadata to create a comprehensive map data object.
    """
    # Load user_layer_matching.json
    layer_owner_id = fetch_layer_owner(req.prdcer_lyr_id)
    
    # Load user data
    layer_owner_data = load_user_data(layer_owner_id)

    if req.prdcer_lyr_id not in layer_owner_data.get("prdcer", {}).get("prdcer_lyrs", {}):
        raise HTTPException(
            status_code=404, detail="Producer layer not found for this user"
        )

    layer_metadata = layer_owner_data["prdcer"]["prdcer_lyrs"][req.prdcer_lyr_id]

    # Load dataset_layer_matching.json
    with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
        dataset_layer_matching = json.load(f)

    # Find the corresponding dataset_id
    dataset_id = None
    for d_id, d_info in dataset_layer_matching.items():
        if req.prdcer_lyr_id in d_info["prdcer_lyrs"]:
            dataset_id = d_id
            break

    if not dataset_id:
        raise HTTPException(status_code=404, detail="Dataset not found for this layer")

    # Load the dataset
    dataset = load_dataset(dataset_id)

    # Transform the dataset
    trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)

    # Combine the transformed dataset with the layer metadata
    result = PrdcerLyrMapData(
        type="FeatureCollection",
        features=trans_dataset["features"],
        prdcer_layer_name=layer_metadata["prdcer_layer_name"],
        prdcer_lyr_id=req.prdcer_lyr_id,
        bknd_dataset_id=dataset_id,
        points_color=layer_metadata["points_color"],
        layer_legend=layer_metadata["layer_legend"],
        layer_description=layer_metadata["layer_description"],
        records_count=d_info["records_count"],
        is_zone_lyr="false",  # Assuming this is always false as per your previous implementation
    )

    return result

def fetch_layer_owner(prdcer_lyr_id:str):
    with open(USER_LAYER_MATCHING_PATH, "r") as f:
        user_layer_matching = json.load(f)
    # Find the owner of the requested layer
    layer_owner_id = user_layer_matching.get(prdcer_lyr_id)
    if not layer_owner_id:
        raise HTTPException(status_code=404, detail="Layer owner not found")
    return layer_owner_id

def load_user_data(user_id:str):
    user_file_path = os.path.join(USERS_PATH, f"user_{user_id}.json")
    if not os.path.exists(user_file_path):
        raise HTTPException(status_code=404, detail="User not found")

    with open(user_file_path, "r") as f:
        user_data = json.load(f)
    return user_data


async def create_save_prdcer_ctlg(req: ReqSavePrdcerCtlg) -> str:
    """
    Creates and saves a new producer catalog. This function updates the user's
    data file with the new catalog information. It ensures that the catalog
    is properly associated with the user and contains all necessary metadata.
    """



    user_file_path = os.path.join(USERS_PATH, f"user_{req.user_id}.json")

    # Load or create user data
    if os.path.exists(user_file_path):
        with open(user_file_path, "r") as f:
            user_data = json.load(f)
    else:
        user_data = {"prdcer": {"prdcer_ctlgs": {}}}

    # Ensure the prdcer and prdcer_ctlgs keys exist
    if "prdcer" not in user_data:
        user_data["prdcer"] = {}
    if "prdcer_ctlgs" not in user_data["prdcer"]:
        user_data["prdcer"]["prdcer_ctlgs"] = {}

    # Add the new producer catalog
    new_catalog = {
        "prdcer_ctlg_name": req.prdcer_ctlg_name,
        "prdcer_ctlg_id": req.prdcer_ctlg_id,
        "subscription_price": req.subscription_price,
        "ctlg_description": req.ctlg_description,
        "total_records": req.total_records,
        "lyrs": req.lyrs,
        "thumbnail_url": req.thumbnail_url,  # Add this line
        "ctlg_owner_user_id": req.user_id,
    }
    user_data["prdcer"]["prdcer_ctlgs"][req.prdcer_ctlg_id] = new_catalog

    # Save updated user data
    with open(user_file_path, "w") as f:
        json.dump(user_data, f, indent=2)

    return "Producer catalog created successfully"


async def fetch_prdcer_ctlgs(req: UserIdRequest) -> list[UserCatalogInfo]:
    """
    Retrieves all producer catalogs associated with a specific user. It reads
    the user's data file and compiles a list of all catalogs owned by the user,
    including metadata like catalog name, description, and associated layers.
    """


    user_file_path = os.path.join(USERS_PATH, f"user_{req.user_id}.json")

    if not os.path.exists(user_file_path):
        return []  # Return an empty list if the user file doesn't exist

    with open(user_file_path, "r") as f:
        user_data = json.load(f)

    user_catalogs = []
    for ctlg_id, ctlg_data in (
        user_data.get("prdcer", {}).get("prdcer_ctlgs", {}).items()
    ):
        user_catalogs.append(
            UserCatalogInfo(
                prdcer_ctlg_id=ctlg_id,
                prdcer_ctlg_name=ctlg_data["prdcer_ctlg_name"],
                ctlg_description=ctlg_data["ctlg_description"],
                thumbnail_url=ctlg_data.get(
                    "thumbnail_url", ""
                ),  # Add a default thumbnail URL if not present
                subscription_price=ctlg_data["subscription_price"],
                total_records=ctlg_data["total_records"],
                lyrs=ctlg_data["lyrs"],
            )
        )

    return user_catalogs


async def fetch_ctlg_lyrs(req: ReqFetchCtlgLyrs) -> list[PrdcerLyrMapData]:
    """
    Fetches all layers associated with a specific catalog. This function first
    locates the catalog (either in the user's profile or in store catalogs),
    then retrieves and transforms the data for each layer in the catalog. It
    compiles these layers into a list of map data objects.
    """

    user_file_path = os.path.join(USERS_PATH, f"user_{req.user_id}.json")

    # Check if catalog exists in user profile
    if os.path.exists(user_file_path):
        with open(user_file_path, "r") as f:
            user_data = json.load(f)
        ctlg = (
            user_data.get("prdcer", {}).get("prdcer_ctlgs", {}).get(req.prdcer_ctlg_id)
        )
    else:
        ctlg = None

    # If not in user profile, check store catalogs
    if not ctlg:
        with open(STORE_CATALOGS_PATH, "r") as f:
            store_ctlgs = json.load(f)
        ctlg = next(
            (
                ctlg
                for ctlg in store_ctlgs["store_catalogs"]
                if ctlg["prdcer_ctlg_id"] == req.prdcer_ctlg_id
            ),
            None,
        )

    if not ctlg:
        raise HTTPException(status_code=404, detail="Catalog not found")

    # Load dataset_layer_matching
    with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
        dataset_layer_matching = json.load(f)

    # Load catalog owner's user data
    ctlg_owner_file_path = os.path.join(
        USERS_PATH, f"user_{ctlg['ctlg_owner_user_id']}.json"
    )
    with open(ctlg_owner_file_path, "r") as f:
        ctlg_owner_data = json.load(f)

    ctlg_lyrs = []
    for lyr_id in ctlg["lyrs"]:
        # Find the corresponding dataset_id
        dataset_id = None
        for d_id, lyr_info in dataset_layer_matching.items():
            if lyr_id in lyr_info["prdcer_lyrs"]:
                dataset_id = d_id
                break

        # Load the dataset
        dataset = load_dataset(dataset_id)

        # Transform the dataset
        trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)

        # find the user who owns this catalog,
        # so we need to have ctlg_owner_user_id to catalog metadata as well as store_catalog
        # Get layer metadata from user profile or use default values
        lyr_metadata = (
            ctlg_owner_data.get("prdcer", {}).get("prdcer_lyrs", {}).get(lyr_id, {})
        )

        ctlg_lyrs.append(
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

    return ctlg_lyrs


async def apply_zone_layers(req: ReqApplyZoneLayers) -> List[PrdcerLyrMapData]:
    """
    Applies zone layer transformations to a set of layers. This complex function
    separates zone and non-zone layers, fetches data for all layers, and then
    applies zone transformations. It creates new layers based on the zone
    properties, effectively segmenting the non-zone points into categories
    based on their proximity to zone points and the values of the zone property.
    """


    # Separate zone layers and non-zone layers
    non_zone_layers = req.lyrs.copy()
    zone_layers = []
    for layer in req.lyrs_as_zone:
        zone_lyr_id = list(layer.keys())[0]
        zone_property_key = list(layer.values())[0]
        zone_layers.append(zone_lyr_id)
        non_zone_layers.remove(zone_lyr_id)

    # Get all data points for non-zone layers
    non_zone_data = []
    for lyr_id in non_zone_layers:
        dataset_id = find_dataset_id(lyr_id)
        if dataset_id:
            dataset = load_dataset(dataset_id)
            lyr_data = await MapBoxConnector.new_ggl_to_boxmap(dataset)
            non_zone_data.extend(lyr_data["features"])

    # Get all data points for zone layers
    zone_data = {}
    for lyr_id in zone_layers:
        dataset_id = find_dataset_id(lyr_id)
        if dataset_id:
            dataset = load_dataset(dataset_id)
            lyr_data = await MapBoxConnector.new_ggl_to_boxmap(dataset)
            zone_data[lyr_id] = lyr_data

    # Apply transformation for each zone layer
    transformed_layers = []
    for layer in req.lyrs_as_zone:
        zone_lyr_id = list(layer.keys())[0]
        zone_property_key = list(layer.values())[0]
        zone_transformed = apply_zone_transformation(
            zone_data[zone_lyr_id], non_zone_data, zone_property_key, zone_lyr_id
        )
        transformed_layers.extend(zone_transformed)

    return transformed_layers


def find_dataset_id(lyr_id: str) -> str:
    """
    Searches for the dataset ID associated with a given layer ID. This function
    reads the dataset-layer matching file and iterates through it to find the
    corresponding dataset for a given layer.
    """

    # Load dataset_layer_matching
    with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
        dataset_layer_matching = json.load(f)
    d_id = None
    for d_id, lyr_info in dataset_layer_matching.items():
        if lyr_id in lyr_info["prdcer_lyrs"]:
            return d_id
    return d_id


def load_dataset(dataset_id: str) -> Dict:
    """
    Loads a dataset from file based on its ID.
    
    """
    dataset_filepath = os.path.join(DATASETS_PATH, f"{dataset_id}.json")
    with open(dataset_filepath, "r") as f:
        return json.load(f)


def apply_zone_transformation(zone_layer_data: Dict, non_zone_points: List, zone_property: str, zone_lyr_id: str) -> List[PrdcerLyrMapData]:
    """
    This function applies zone transformations to a set of points. It first
    calculates thresholds based on the zone property values. Then, it creates
    new layers and distributes non-zone points into these layers based on their
    proximity to zone points and the thresholds. This results in a segmentation
    of points into different categories (low, medium, high, and non-overlapping).
    """
    # Extract property values and calculate thresholds
    zone_property = zone_property.split("features.properties.")[-1]
    property_values = [
        feature["properties"].get(zone_property, 9191919191)
        for feature in zone_layer_data["features"]
    ]  # TODO this is about the slowest thing probably
    # Convert the list to a NumPy array
    arr = np.array(property_values)
    avg = np.mean(arr[arr != 9191919191.0])
    new_arr = np.where(arr == 9191919191.0, avg, arr)
    property_values = new_arr.tolist()
    thresholds = calculate_thresholds(property_values)

    # Create 4 new layers
    new_layers = [
        PrdcerLyrMapData(
            type="FeatureCollection",
            features=[],
            prdcer_layer_name=f"{zone_layer_data.get('prdcer_layer_name', 'Layer')} ({category})",
            prdcer_lyr_id=f"zy{zone_lyr_id}_applied_{i+1}",
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

    # Distribute non-zone points to new layers based on zone layer
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
                break  # Stop checking other zone points once we've found a match
            else:
                new_layers[3].features.append(create_feature(point))

    # Update records count
    for layer in new_layers:
        layer.records_count = len(layer.features)

    return new_layers



def create_feature(point):
    """
    Converts a point dictionary into a Feature object. This function is used
    to ensure that all points are in the correct format for geospatial operations.
    """


    return Feature(
        type=point["type"],
        properties=point["properties"],
        geometry=Geometry(type="Point", coordinates=point["geometry"]["coordinates"]),
    )


def calculate_thresholds(values):
    """
    Calculates threshold values to divide a set of values into three categories.
    It sorts the values and returns two threshold points that divide the data
    into thirds.
    """
    sorted_values = sorted(values)
    n = len(sorted_values)
    return [sorted_values[n // 3], sorted_values[2 * n // 3]]


def calculate_distance_km(point1, point2):
    """
    Calculates the distance between two points in kilometers using the Haversine formula.

    """
    # Earth's radius in kilometers
    R = 6371

    # Convert latitude and longitude to radians
    lon1, lat1 = math.radians(point1[0]), math.radians(point1[1])
    lon2, lat2 = math.radians(point2[0]), math.radians(point2[1])

    # Differences in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Calculate the distance
    distance = R * c

    return distance
