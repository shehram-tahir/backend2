import requests

from all_types.myapi_dtypes import ReqLocation
from config_factory import get_conf

CONF = get_conf()

# async def fetch_from_google_maps_api(location_req: LocationRequest):
#     lat, lng, radius, place_type = location_req.lat, location_req.lng, location_req.radius, location_req.type
#     params = {
#         'key': CONF.api_key,
#         'location': f"{lat},{lng}",
#         'radius': radius,
#         'type': place_type
#     }

#     response = requests.get(CONF.nearby_search, params=params)
#     results = response.json()

#     output_data = []
#     if 'results' in results:
#         for place in results['results']:
#             details_params = {
#                 'key': CONF.api_key,
#                 'place_id': place['place_id'],
#                 'fields': CONF.google_fields
#             }
#             details_response = requests.get(CONF.place_details, params=details_params)
#             place_details = details_response.json().get('result', {})
#             output_data.append(place_details)

#     return output_data


async def fetch_from_google_maps_api(req: ReqLocation):
    lat, lng, radius, place_type, page_token = req.lat, req.lng, req.radius, req.type, req.page_token

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": CONF.api_key,
        "X-Goog-FieldMask": CONF.google_fields
    }
    data = {
        "includedTypes": [place_type],
        "locationRestriction": {
            "circle": {
                "center": {
                    "latitude": lat,
                    "longitude": lng
                },
                "radius": radius
            }
        }
    }

    if page_token:
        data["pageToken"] = page_token

    response = requests.post(CONF.nearby_search, headers=headers, json=data)
    if response.status_code == 200:
        response_data = response.json()
        results = response_data.get("places", [])
        next_page_token = response_data.get("nextPageToken")
        return results, next_page_token
    else:
        print("Error:", response.status_code)
        return [], None

# import json
# with open("Backend/datasets/111.json", "w") as file:
#     json.dump(results, file, indent=4)
