from all_types.google_dtypes import GglResponse
from all_types.myapi_dtypes import LocationReq, CatlogId
from all_types.myapi_dtypes import (
    CountryCityData,
    ReqSavePrdcerLyer,
    UserLayerInfo,
    UserIdRequest,
    PrdcerLyrMapData,
    ReqSavePrdcerCtlg,
    UserCatalogInfo
)
from google_api_connector import fetch_from_google_maps_api
from mapbox_connector import MapBoxConnector
from storage import (
    get_data_from_storage,
    store_data,
    get_dataset_from_storage,
    search_metastore_for_string,
)
import asyncio
import os
import json
from fastapi import HTTPException
import uuid
import json
import os


async def fetch_nearby(location_req: LocationReq):
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
    data = await get_dataset_from_storage(catalogue_dataset_id)
    if not data:
        data = {}
    return data


async def fetch_catlog_collection(**kwargs):
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
    response_data: GglResponse = await get_catalogue_dataset(req.catalogue_dataset_id)
    trans_data = await MapBoxConnector.ggl_to_boxmap(response_data)
    return trans_data


async def nearby_boxmap(req):
    response_data = await fetch_nearby(req)
    trans_data = await MapBoxConnector.new_ggl_to_boxmap(response_data)
    return trans_data


async def fetch_country_city_data(**kwargs):
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
    dataset_category = req.dataset_category
    dataset_country = req.dataset_country
    dataset_city = req.dataset_city
    layer_filename = f"{dataset_category}_{dataset_country}_{dataset_city}.json"
    existing_layer = await search_metastore_for_string(layer_filename)
    if existing_layer:
        bknd_dataset_id = existing_layer["bknd_dataset_id"]
        dataset_filename = f"{bknd_dataset_id}.json"
        DATASETS_PATH = "Backend/datasets"
        dataset_filepath = os.path.join(DATASETS_PATH, dataset_filename)
        with open(dataset_filepath, "r") as f:
            dataset = json.load(f)

        trans_dataset = await MapBoxConnector.new_ggl_to_boxmap(dataset)
        trans_dataset["bknd_dataset_id"] = bknd_dataset_id
        trans_dataset["records_count"] = len(trans_dataset["features"])
        return trans_dataset
    else:
        # code to create layer
        pass


async def create_save_prdcer_lyr(req: ReqSavePrdcerLyer):
    USERS_PATH = "Backend/users"
    DATASET_LAYER_MATCHING_PATH = "Backend/dataset_layer_matching.json"

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

    # Update dataset_layer_matching.json
    if os.path.exists(DATASET_LAYER_MATCHING_PATH):
        with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
            dataset_layer_matching = json.load(f)
    else:
        dataset_layer_matching = {}

    if req.bknd_dataset_id not in dataset_layer_matching:
        dataset_layer_matching[req.bknd_dataset_id] = []

    if req.prdcer_lyr_id not in dataset_layer_matching[req.bknd_dataset_id]:
        dataset_layer_matching[req.bknd_dataset_id].append(req.prdcer_lyr_id)

    with open(DATASET_LAYER_MATCHING_PATH, "w") as f:
        json.dump(dataset_layer_matching, f, indent=2)

    return "Producer layer created successfully"


async def fetch_prdcer_lyrs(req: UserIdRequest) -> list[UserLayerInfo]:
    user_id = req.user_id
    DATASET_LAYER_MATCHING_PATH = "Backend/dataset_layer_matching.json"
    DATASETS_PATH = "Backend/datasets"
    USERS_PATH = "Backend/users"
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


async def fetch_prdcer_lyr_map_data(req: PrdcerLyrMapData):
    USERS_PATH = "Backend/users"
    DATASET_LAYER_MATCHING_PATH = "Backend/dataset_layer_matching.json"
    DATASETS_PATH = "Backend/datasets"

    # Load user data
    user_file_path = os.path.join(USERS_PATH, f"user_{req.user_id}.json")
    if not os.path.exists(user_file_path):
        raise HTTPException(status_code=404, detail="User not found")

    with open(user_file_path, 'r') as f:
        user_data = json.load(f)

    if req.prdcer_lyr_id not in user_data.get("prdcer", {}).get("prdcer_lyrs", {}):
        raise HTTPException(status_code=404, detail="Producer layer not found for this user")

    layer_metadata = user_data["prdcer"]["prdcer_lyrs"][req.prdcer_lyr_id]

    # Load dataset_layer_matching.json
    with open(DATASET_LAYER_MATCHING_PATH, 'r') as f:
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
    dataset_filepath = os.path.join(DATASETS_PATH, f"{dataset_id}.json")
    with open(dataset_filepath, 'r') as f:
        dataset = json.load(f)

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
        is_zone_lyr="false"  # Assuming this is always false as per your previous implementation
    )

    return result


async def create_save_prdcer_ctlg(req: ReqSavePrdcerCtlg) -> str:
    USERS_PATH = "Backend/users"
    user_file_path = os.path.join(USERS_PATH, f"user_{req.user_id}.json")

    # Load or create user data
    if os.path.exists(user_file_path):
        with open(user_file_path, 'r') as f:
            user_data = json.load(f)
    else:
        user_data = {"prdcer": {"prdcer_ctlgs": {}}}

    # Ensure the prdcer and prdcer_ctlgs keys exist
    if "prdcer" not in user_data:
        user_data["prdcer"] = {}
    if "prdcer_ctlgs" not in user_data["prdcer"]:
        user_data["prdcer"]["prdcer_ctlgs"] = {}

    # Add the new producer catalog
    user_data["prdcer"]["prdcer_ctlgs"][req.prdcer_ctlg_id] = {
        "prdcer_ctlg_name": req.prdcer_ctlg_name,
        "prdcer_ctlg_id": req.prdcer_ctlg_id,
        "subscription_price": req.subscription_price,
        "ctlg_description": req.ctlg_description,
        "total_records": req.total_records,
        "lyrs": req.lyrs,
        "thumbnail_url": req.thumbnail_url  # Add this line
    }

    # Save updated user data
    with open(user_file_path, 'w') as f:
        json.dump(user_data, f, indent=2)

    return "Producer catalog created successfully"


async def fetch_prdcer_ctlgs(req: UserIdRequest) -> list[UserCatalogInfo]:
    USERS_PATH = "Backend/users"
    user_file_path = os.path.join(USERS_PATH, f"user_{req.user_id}.json")

    if not os.path.exists(user_file_path):
        return []  # Return an empty list if the user file doesn't exist

    with open(user_file_path, 'r') as f:
        user_data = json.load(f)

    user_catalogs = []
    for ctlg_id, ctlg_data in user_data.get("prdcer", {}).get("prdcer_ctlgs", {}).items():
        user_catalogs.append(
            UserCatalogInfo(
                prdcer_ctlg_id=ctlg_id,
                prdcer_ctlg_name=ctlg_data["prdcer_ctlg_name"],
                ctlg_description=ctlg_data["ctlg_description"],
                thumbnail_url=ctlg_data.get("thumbnail_url", ""),  # Add a default thumbnail URL if not present
                subscription_price=ctlg_data["subscription_price"],
                total_records=ctlg_data["total_records"],
                lyrs=ctlg_data["lyrs"]
            )
        )

    return user_catalogs








































