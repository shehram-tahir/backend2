import json
import os
import uuid
from datetime import datetime
from typing import Dict, Union, Tuple, Optional

from fastapi import HTTPException

from all_types.myapi_dtypes import ReqLocation
from config_factory import get_conf

USERS_PATH = "Backend/users"
STORE_CATALOGS_PATH = "Backend/store_catalogs.json"
DATASET_LAYER_MATCHING_PATH = "Backend/dataset_layer_matching.json"
DATASETS_PATH = "Backend/datasets"
USER_LAYER_MATCHING_PATH = "Backend/user_layer_matching.json"
METASTORE_PATH = "Backend/layer_category_country_city_matching"
STORAGE_DIR = "Backend/storage"
USERS_INFO_PATH = "Backend/users_info.json"

CONF = get_conf()
os.makedirs(STORAGE_DIR, exist_ok=True)


def get_filename(location_req: ReqLocation) -> str:
    """
    Generates a filename based on the location request parameters.
    """
    lat, lng, radius, place_type = (
        location_req.lat,
        location_req.lng,
        location_req.radius,
        location_req.type,
    )
    return f"{STORAGE_DIR}/data_{lat}_{lng}_{radius}_{place_type}.json"


async def get_data_from_storage(location_req: ReqLocation) -> Union[Dict, None]:
    """
    Retrieves data from storage based on the location request.
    """
    filename = get_filename(location_req)
    if os.path.exists(filename):
        with open(filename, "r") as file:
            return json.load(file)
    return None


async def get_dataset_from_storage(dataset_id: str) -> Union[Dict, None]:
    """
    Retrieves a dataset from storage based on the dataset ID.
    """
    filename = f"{STORAGE_DIR}/catalogue_data_{dataset_id}.json"
    if os.path.exists(filename):
        with open(filename, "r") as file:
            return json.load(file)
    return None


async def store_data(location_req: ReqLocation, data) -> None:
    """
    Stores data in a file based on the location request.
    """
    filename = get_filename(location_req)
    with open(filename, "w") as file:
        json.dump(data, file)


async def search_metastore_for_string(string_search: str) -> Union[Dict, None]:
    """
    Searches the metastore for a given string and returns the corresponding data if found.
    """
    meta_file_path = os.path.join(METASTORE_PATH, string_search)
    if os.path.exists(meta_file_path):
        with open(meta_file_path, "r") as f:
            return json.load(f)
    return None


def load_dataset_layer_matching():
    # Load dataset_layer_matching
    with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
        dataset_layer_matching = json.load(f)
    return dataset_layer_matching


def fetch_dataset_id(lyr_id: str,
                     dataset_layer_matching: dict = load_dataset_layer_matching()) -> Tuple[str, dict]:
    """
    Searches for the dataset ID associated with a given layer ID. This function
    reads the dataset-layer matching file and iterates through it to find the
    corresponding dataset for a given layer.
    """

    d_id = None
    for d_id, dataset_info in dataset_layer_matching.items():
        if lyr_id in dataset_info["prdcer_lyrs"]:
            return d_id, dataset_info
    if not d_id:
        raise HTTPException(status_code=404, detail="Dataset not found for this layer")


def load_dataset(dataset_id: str) -> Dict:
    """
    Loads a dataset from file based on its ID.

    """
    dataset_filepath = os.path.join(DATASETS_PATH, f"{dataset_id}.json")
    with open(dataset_filepath, "r") as f:
        return json.load(f)


def fetch_layer_owner(prdcer_lyr_id: str) -> str:
    """
    Fetches the owner of a layer based on the producer layer ID.
    """
    with open(USER_LAYER_MATCHING_PATH, "r") as f:
        user_layer_matching = json.load(f)
    # Find the owner of the requested layer
    layer_owner_id = user_layer_matching.get(prdcer_lyr_id)
    if not layer_owner_id:
        raise HTTPException(status_code=404, detail="Layer owner not found")
    return layer_owner_id


def load_user_profile(user_id: str) -> Dict:
    """
    Loads user data from a file based on the user ID.
    """
    user_file_path = os.path.join(USERS_PATH, f"user_{user_id}.json")
    if not os.path.exists(user_file_path):
        # create a new user profile
        create_new_user("0000", "temp_user", "temp@email")
        user_file_path = os.path.join(USERS_PATH, f"user_0000.json")

    with open(user_file_path, "r") as f:
        user_data = json.load(f)
    return user_data


def update_dataset_layer_matching(prdcer_lyr_id: str, bknd_dataset_id: str, records_count: int = 9191919):
    if os.path.exists(DATASET_LAYER_MATCHING_PATH):
        with open(DATASET_LAYER_MATCHING_PATH, "r") as f:
            dataset_layer_matching = json.load(f)

    if bknd_dataset_id not in dataset_layer_matching:
        dataset_layer_matching[bknd_dataset_id] = {
            "records_count": records_count,
            "prdcer_lyrs": []
        }

    if prdcer_lyr_id not in dataset_layer_matching[bknd_dataset_id]["prdcer_lyrs"]:
        dataset_layer_matching[bknd_dataset_id]["prdcer_lyrs"].append(prdcer_lyr_id)

    # Update records_count if it has changed
    dataset_layer_matching[bknd_dataset_id]["records_count"] = records_count

    with open(DATASET_LAYER_MATCHING_PATH, "w") as f:
        json.dump(dataset_layer_matching, f, indent=2)


def update_user_layer_matching(layer_id, layer_owner_id):
    # Update user_layer_matching.json
    with open(USER_LAYER_MATCHING_PATH, 'r+') as f:
        user_layer_matching = json.load(f)
        user_layer_matching[layer_id] = layer_owner_id
        f.seek(0)
        json.dump(user_layer_matching, f, indent=2)
        f.truncate()


def update_user_profile(user_id, user_data):
    user_file_path = os.path.join(USERS_PATH, f"user_{user_id}.json")
    with open(user_file_path, "w") as f:
        json.dump(user_data, f, indent=2)


def fetch_user_layers(user_id):
    user_data = load_user_profile(user_id)
    user_layers = user_data.get("prdcer", {}).get("prdcer_lyrs", {})
    return user_layers


def fetch_user_catalogs(user_id):
    user_data = load_user_profile(user_id)
    user_catalogs = user_data.get("prdcer", {}).get("prdcer_ctlgs", {})
    return user_catalogs


def create_new_user(user_id, username, email):
    user_file_path = os.path.join(USERS_PATH, f"user_{user_id}.json")

    if os.path.exists(user_file_path):
        raise HTTPException(status_code=400, detail="User profile already exists")

    user_data = {
        "user_id": user_id,
        "username": username,
        "email": email,
        "prdcer": {"prdcer_lyrs": {}, "prdcer_ctlgs": {}},
    }

    with open(user_file_path, "w") as f:
        json.dump(user_data, f, indent=2)


def load_store_catalogs():
    with open(STORE_CATALOGS_PATH, "r") as f:
        store_ctlgs = json.load(f)
    return store_ctlgs


def save_dataset(bknd_dataset_id: str, dataset: dict):
    """Save the dataset to a file"""
    with open(f"{DATASETS_PATH}/{bknd_dataset_id}.json", 'w') as f:
        json.dump(dataset, f)


def update_metastore(ccc_filename: str, bknd_dataset_id: str):
    """Update the metastore with the new layer information"""
    if bknd_dataset_id is not None:
        metastore_data = {
            "bknd_dataset_id": bknd_dataset_id,
            "created_at": datetime.now().isoformat()
        }
        with open(f"{METASTORE_PATH}/{ccc_filename}", 'w') as f:
            json.dump(metastore_data, f)


def get_country_code(country_name: str) -> str:
    country_codes = {
        "United Arab Emirates": "AE",
        "Saudi Arabia": "SA",
        "Canada": "CA"
    }
    return country_codes.get(country_name, "")


async def fetch_country_city_data(**_):
    """
    Returns a set of country and city data for United Arab Emirates, Saudi Arabia, and Canada.
    The data is structured as a dictionary where keys are country names and values are lists of cities.
    """
    data = {
        "United Arab Emirates": [
            {"name": "Dubai", "lat": 25.2048, "lng": 55.2708},
            {"name": "Abu Dhabi", "lat": 24.4539, "lng": 54.3773},
            {"name": "Sharjah", "lat": 25.3573, "lng": 55.4033},
        ],
        "Saudi Arabia": [
            {"name": "Riyadh", "lat": 24.7136, "lng": 46.6753},
            {"name": "Jeddah", "lat": 21.5433, "lng": 39.1728},
            {"name": "Mecca", "lat": 21.4225, "lng": 39.8262},
        ],
        "Canada": [
            {"name": "Toronto", "lat": 43.6532, "lng": -79.3832},
            {"name": "Vancouver", "lat": 49.2827, "lng": -123.1207},
            {"name": "Montreal", "lat": 45.5017, "lng": -73.5673},
        ],
    }
    return data


async def fetch_nearby_categories(**_):
    """
    Provides a comprehensive list of nearby place categories, organized into
    broader categories. This function returns a large, predefined dictionary
    of categories and subcategories, covering various aspects of urban life
    such as automotive, culture, education, entertainment, and more.
    """
    with open('Backend/google_categories.json', 'r') as f:
        categories = json.load(f)
    return categories


def generate_user_id() -> str:
    return str(uuid.uuid4())


def load_users_info() -> Dict:
    if os.path.exists(USERS_INFO_PATH):
        with open(USERS_INFO_PATH, 'r') as f:
            return json.load(f)
    return {"users": []}


def save_users_info(users_info: Dict):
    with open(USERS_INFO_PATH, 'w') as f:
        json.dump(users_info, f, indent=2)


def is_username_or_email_taken(username: str, email: str) -> bool:
    users_info = load_users_info()
    for user in users_info["users"]:
        if user["username"] == username or user["email"] == email:
            return True
    return False


def add_user_to_info(user_id: str, username: str, email: str, hashed_password: str):
    users_info = load_users_info()
    users_info["users"].append({
        "user_id": user_id,
        "username": username,
        "email": email,
        "hashed_password": hashed_password
    })
    save_users_info(users_info)


def get_user_by_username(username: str) -> Optional[Dict]:
    users_info = load_users_info()
    for user in users_info["users"]:
        if user["username"] == username:
            return user
    return None
