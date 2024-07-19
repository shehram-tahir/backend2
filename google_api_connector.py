import requests

from all_types.myapi_dtypes import ReqLocation
from config_factory import get_conf

CONF = get_conf()

async def old_fetch_from_google_maps_api(req: ReqLocation):
    lat, lng, radius, place_type, page_token = req.lat, req.lng, req.radius, req.type, req.page_token
    params = {
        'key': CONF.api_key,
        'location': f"{lat},{lng}",
        'radius': radius,
        'type': place_type
    }

    response = requests.get(CONF.nearby_search, params=params)
    results = response.json()

    output_data = []
    if 'results' in results:
        for place in results['results']:
            details_params = {
                'key': CONF.api_key,
                'place_id': place['place_id'],
                'fields': CONF.google_fields
            }
            details_response = requests.get(CONF.place_details, params=details_params)
            place_details = details_response.json().get('result', {})
            output_data.append(place_details)

    return output_data


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

import math
from typing import List, Tuple
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

def generate_grid_points(center_lat: float, center_lng: float, radius_km: float, step_km: float = 0.1) -> List[Tuple[float, float]]:
    """Generate a grid of points within the given radius."""
    points = []
    steps = int(radius_km / step_km)
    for i in range(-steps, steps + 1):
        for j in range(-steps, steps + 1):
            lat = center_lat + (i * step_km / 111.32)  # 1 degree of latitude = 111.32 km
            lng = center_lng + (j * step_km / (111.32 * math.cos(math.radians(center_lat))))
            if calculate_distance_km((center_lng, center_lat), (lng, lat)) <= radius_km:
                points.append((lat, lng))
    return points

async def grid_fetch_from_google_maps_api(req: ReqLocation):
    lat, lng, radius, place_type = req.lat, req.lng, req.radius, req.type

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": CONF.api_key,
        "X-Goog-FieldMask": CONF.google_fields
    }

    all_results = []
    grid_points = generate_grid_points(lat, lng, radius / 1000, 0.1)  # Convert radius to km

    for point_lat, point_lng in grid_points:
        data = {
            "includedTypes": [place_type],
            "locationRestriction": {
                "circle": {
                    "center": {
                        "latitude": point_lat,
                        "longitude": point_lng
                    },
                    "radius": 100  # 100 meters
                }
            }
        }

        response = requests.post(CONF.nearby_search, headers=headers, json=data)
        if response.status_code == 200:
            response_data = response.json()
            results = response_data.get("places", [])
            all_results.extend(results)
        else:
            print(f"Error for point ({point_lat}, {point_lng}): {response.status_code}")

    # Remove duplicates based on 'id'
    unique_results = {result['id']: result for result in all_results}.values()

    return list(unique_results), None  # Return None for next_page_token as we're not using pagination

