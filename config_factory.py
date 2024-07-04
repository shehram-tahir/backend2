import json
from dataclasses import dataclass, fields, is_dataclass


@dataclass
class static_ApiConfig:
    api_key: str = ""
    ggl_base_url: str = "https://places.googleapis.com/v1/places:"
    nearby_search: str = ggl_base_url + "searchNearby"
    place_details: str = ggl_base_url + "details/json"
    enable_CORS_url: str = "http://localhost:3000"
    catlog_collection: str = "/fastapi/catlog_collection"
    layer_collection: str = "/fastapi/layer_collection"
    fetch_acknowlg_id: str = "/fastapi/fetch_acknowlg_id"
    catlog_data: str = "/fastapi/ws_dataset_load/{request_id}"
    http_catlog_data: str = "/fastapi/http_catlog_data"
    single_nearby: str = "/fastapi/ws/{request_id}"
    http_single_nearby: str = "/fastapi/http_single_nearby"
    country_city: str = "/fastapi/country_city"
    nearby_categories: str = "/fastapi/nearby_categories"
    old_nearby_categories: str = "/fastapi/old_nearby_categories"
    create_layer: str = "/fastapi/create_layer"
    save_producer_layer:str = "/fastapi/save_producer_layer"
    user_layers: str = "/fastapi/user_layers"
    prdcer_lyr_map_data: str = "/fastapi/prdcer_lyr_map_data"
    save_producer_catalog: str = "/fastapi/save_producer_catalog"
    user_catalogs: str = "/fastapi/user_catalogs"
    fetch_ctlg_lyrs: str = "/fastapi/fetch_ctlg_lyrs"
    apply_zone_layers: str = "/fastapi/apply_zone_layers"    
    google_fields: str = "places.id,places.types,places.location,places.rating,places.priceLevel,places.userRatingCount,places.displayName,places.primaryType,places.formattedAddress,places.takeout,places.delivery,places.paymentOptions"


@dataclass
class ConfigDict:
    def __init__(self, dictionary):
        for key, value in dictionary.items():
            if isinstance(value, dict):
                setattr(self, key, ConfigDict(value))
            else:
                setattr(self, key, value)


class ConfigFactory:
    @staticmethod
    def load_config(file_path: str, config_class):
        # Load configuration from a JSON file
        with open(file_path, "r") as config_file:
            config_data = json.load(config_file)

        # Recursively convert dictionaries to instances of the config class
        return ConfigFactory._dict_to_object(config_data, config_class)

    @staticmethod
    def _dict_to_object(data, config_class):
        if not is_dataclass(config_class):
            raise TypeError(f"{config_class} is not a dataclass type")

        # Create an instance of the config class
        obj = config_class(**{f.name: None for f in fields(config_class)})

        for key, value in data.items():
            if isinstance(value, dict):
                # Set nested dictionaries as ConfigDict objects for attribute access
                setattr(obj, key, ConfigDict(value))
            else:
                setattr(obj, key, value)

        return obj


# Example usage
# Assuming the JSON configuration file looks something like this:
# {
#     "api_key": "your_api_key",
#     "base_urls": {
#         "google": "https://maps.googleapis.com/maps/api",
#         "bing": "https://dev.virtualearth.net/REST/v1"
#     }
# }
# # Loading configuration
# config = ConfigFactory.load_config(
#     'G:\\My Drive\\Personal\\Work\\offline\\Jupyter\\Git\\s_locator\\common_settings.json', ApiCommonConfig)
# print(config.api_key)  # Output: your_api_key
# print(config.base_urls.google)  # Output: https://maps.googleapis.com/maps/api


def get_conf():
    conf = static_ApiConfig()
    try:
        with open("secrets.json", "r") as config_file:
            conf.api_key = json.load(config_file).get("gmaps_api", "")
    except:
        conf.api_key = ""

    return conf
