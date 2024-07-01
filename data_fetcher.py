from all_types.google_dtypes import GglResponse
from all_types.myapi_dtypes import LocationReq, CatlogId
from all_types.myapi_dtypes import CountryCityData
from google_api_connector import fetch_from_google_maps_api
from mapbox_connector import MapBoxConnector
from storage import get_data_from_storage, store_data, get_dataset_from_storage
import asyncio


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
