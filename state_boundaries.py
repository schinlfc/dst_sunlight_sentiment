"""Library for downloading and loading shapefile for state boundaries as
a geopandas dataframe.

Also performs some cleaning of the shapefile."""

import geopandas as gpd
from functools import lru_cache
import commentjson as cjson
import json
import re
import util


@util.gdf_file_cache('shape/states.shp')
def get_states():
    url = 'https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip'
    filename = util.script_relative('shape/cb_2018_us_state_500k.zip')
    util.maybe_download_file(url, filename)
    states = gpd.read_file('zip://' + filename)
    # These entities are included in the state shapefile but are
    # not states for our purposes. Drop them.
    non_state_fips = [
        # Note: include DC in the state shapefile, because not having it
        # is causing lots of false positives where tweets appear to match
        # nothing. Filter it later.
        # '11',  # District of Columbia
        '60',  # American Samoa
        '66',  # Guam
        '69',  # Northern Mariana Islands
        '72',  # Puerto Rico
        '78',  # Virgin Islands
    ]
    states = states[~states['STATEFP'].isin(non_state_fips)]
    # Drop every column but STUSPS and geometry
    states = states[['STUSPS', 'geometry']]
    # STUSPS is the official two-letter code used by the postal service for the state
    # e.g. CO for Colorado
    states = states.rename(columns={'STUSPS': 'state_abbr'})
    states = states.sort_values(by='state_abbr').set_index('state_abbr')
    states = states.to_crs('epsg:4326')
    _ = states.sindex
    return states


@lru_cache(None)
def get_state_names():
    with open('state_names.json', 'rb') as f:
        return cjson.load(f)


@lru_cache(None)
def get_place_overrides():
    with open('place_overrides.json', 'rb') as f:
        return cjson.load(f)


def state_name_match(name):
    # Load list of common state names from JSON file, and either
    # map that value to the short code, or return None.
    return get_state_names().get(name)


def city_name_match(name):
    if match := re.match("[A-Za-z' -]+, ([A-Z]{2})$", name):
        state = match.group(1)
        state_codes = get_state_names().values()
        if state not in state_codes:
            # We found something that looks like a two-letter state
            # code but it's not on the list of states. False positive.
            return None
        return state
    else:
        # No match.
        return None


def place_disabled(place):
    place_overrides = get_place_overrides()
    return place['full_name'] in place_overrides['disable']


def place_remapped(place):
    """If place has been remapped, return the remapped value."""
    place_overrides = get_place_overrides()
    return place_overrides['remap'].get(place['full_name'])


def lookup_by_name(place):
    # Return first non-null result
    return state_name_match(place['full_name']) \
        or city_name_match(place['full_name'])


def lookup_by_geospatial(place):
    # Convert from geojson to shapely shape
    poly = util.get_bbox_from_place(place)
    if poly.area == 0:
        poly = poly.buffer(1e-4)
    states = get_states()
    intersections = states.intersects(poly)
    num_intersections = intersections.sum()
    if num_intersections == 0:
        # No intersections
        return None
    elif num_intersections == 1:
        # Single intersection - return first one
        return intersections.loc[intersections].index[0]
    elif num_intersections == 50:
        # Some tweets are "geocoded" to the entire US. Return no match
        # in these cases.
        return None
    else:
        # At least two intersections
        intersecting_states = states.loc[intersections]
        area_percentage = util.get_percentage_overlap(intersecting_states, poly)
        first_state_percent = area_percentage.iloc[0]
        if first_state_percent > 80:
            # This bounding box is at least 80% in one state
            # Return that state
            return area_percentage.index[0]

        # Raise an exception so this can be manually looked at.
        # msg = f'Ambiguous place - debug info:\n{area_percentage}\n' \
        #     f"Name:{repr(place['full_name'])} ID: {repr(place['id'])}"
        # print(gpd.GeoSeries([poly]).to_json())
        # raise Exception(msg)
        return None


def geocode_place_inner(place):
    if not isinstance(place, dict):
        raise Exception(f'expected place, not {type(place)}')
    # Places can be disabled if they're an error place or too ambiguous
    if place_disabled(place):
        return None
    # Before doing any mapping, check if this has an override
    if match := place_remapped(place):
        return match
    # Check if the state name is in the place. e.g. Cleveland, OH
    if match := lookup_by_name(place):
        return match
    # Check the bounding box. If it's mostly in one state, use that.
    if match := lookup_by_geospatial(place):
        return match
    return None


def geocode_place(place):
    state = geocode_place_inner(place)
    if state == 'DC':
        # Remove DC
        state = None
    if state is not None and \
            state not in get_state_names().values():
        raise Exception(f'Unknown state {state}')
    return state


if __name__ == '__main__':
    # with open('places.json', 'rt') as f:
    #     places = [place for place in map(json.loads, f)]

    # get_states()
    # for place in places:
    #     bbox = place['bounding_box']
    #     name = place['full_name']
    #     geo_result = geocode_place(place)
    #     print(geo_result)
    get_states()
