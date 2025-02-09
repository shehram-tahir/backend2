import math
from geopy.distance import geodesic


def get_point_at_distance(start_point, bearing, distance_km):
    return geodesic(kilometers=distance_km).destination(start_point, bearing)
