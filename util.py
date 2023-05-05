import json
import time
import os
import requests
import subprocess
import re
import datetime
import pytz
import geopandas as gpd
from functools import lru_cache
from shapely.geometry import box, shape
import warnings
import myloginpath
import sqlalchemy


def get_usage():
    config = json.load(open('config.json', 'rb'))
    cookies = {
        'auth_token': config['getusage_auth_token'],
    }

    params = {
        'interval': 'past',
    }

    mystery_magic_1 = config['getusage_token1']
    mystery_magic_2 = config['getusage_token2']
    response = requests.get(
        f'https://developer.twitter.com/api/accounts/{mystery_magic_1}/usage',
        params=params,
        cookies=cookies,
    )
    response.raise_for_status()
    usage = response.json()['usage'][mystery_magic_2]['tweets']['usages'][-1]['count']
    cap = response.json()['usage'][mystery_magic_2]['tweets']['cap']
    usage = int(usage)
    cap = int(cap)
    return usage, cap


def script_relative(file_path):
    """Get absolute path, relative to this script's location."""
    script_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(script_path, file_path)


def file_exists_non_zero_size(filename):
    return os.path.isfile(filename) and os.path.getsize(filename) > 0


def maybe_download_file(url, filename):
    """Download a file if it doesn't exist."""
    if file_exists_non_zero_size(filename):
        # We already have this file, and it has at least one byte
        return
    # Stream file to disk
    with requests.get(url, stream=True) as web:
        web.raise_for_status()
        with open(filename, 'wb') as fh:
            for chunk in web.iter_content(chunk_size=128*1024):
                fh.write(chunk)


def gdf_file_cache(filename):
    """Cache decorator which either loads geographic content from a file
    or calls the function it wraps.

    Does not support arguments for the function it wraps."""
    filename = script_relative(filename)

    def inner1(function):
        def inner2():
            if file_exists_non_zero_size(filename):
                gdf = gpd.read_file(filename)
                # Set first column as the index
                gdf = gdf.set_index(gdf.columns[0])
                return gdf
            else:
                # No cache available
                gdf = function()
                gdf.to_file(filename)
                return gdf
        return lru_cache(maxsize=None)(inner2)
    return inner1


def get_percentage_overlap(gdf, poly, cea=True):
    intersection = gdf.intersection(poly)
    if cea:
        # Right now, the intersection is represented by polygons represented
        # in terms of degrees. This needs to be converted to CEA, so that we
        # don't over-weight intersections further away from poles.
        area = intersection.to_crs('+proj=cea').area
    else:
        with warnings.catch_warnings():
            message_to_ignore = 'Geometry is in a geographic CRS.*'
            warnings.filterwarnings('ignore', message=message_to_ignore)
            area = intersection.area
    total_intersection_area = area.sum()
    # Check for division by zero
    assert total_intersection_area != 0
    area_percentage = area / total_intersection_area * 100
    area_percentage = area_percentage.sort_values(ascending=False)
    return area_percentage


class AdaptiveSleepTimer(object):
    def __init__(self):
        self.last_sleep_wakeup = None

    def sleep(self, delay_seconds):
        if self.last_sleep_wakeup is None:
            # This is the first sleep. Sleep the full time.
            delay = delay_seconds
        else:
            # Calculate how long we've spent not sleeping
            time_since_last_wakeup = time.time() - self.last_sleep_wakeup
            delay = delay_seconds - time_since_last_wakeup
            delay = max(0, delay)  # If delay is less than zero, set it to zero
        # print(f'delay: {delay}')
        time.sleep(delay)
        self.last_sleep_wakeup = time.time()


def get_bbox_from_place(place):
    if 'bounding_box' in place and place['bounding_box'] is not None:
        return shape(place['bounding_box'])
    elif 'geo' in place:
        return convert_twitter_bbox_to_polygon(place['geo']['bbox'])
    else:
        raise Exception(f"Can't get bbox from place, place dict {place}")


def convert_twitter_bbox_to_polygon(bbox):
    assert isinstance(bbox, list)
    assert len(bbox) == 4
    return box(*bbox)


def fast_line_count(filename):
    output = subprocess.check_output(['wc', '-l', filename])
    output = output.decode('utf-8')
    pattern = f'^ ?(\\d+) {filename}$'
    m = re.match(pattern, output)
    if m is None:
        raise Exception(f'Failed to match wc output. Output: {repr(output)} Pattern: {repr(pattern)}')
    return int(m.group(1))


def listify(func):
    def inner(*args, **kwargs):
        return list(func(*args, **kwargs))
    return inner


def table_row_count(con, table):
    sql = f'select count(*) from {table}'
    rowcount = con.execute(sql).fetchall()[0][0]
    return rowcount


def parse_twitter_timestamp(timestamp_str):
    assert isinstance(timestamp_str, str)
    assert timestamp_str.endswith('Z')
    timestamp_obj = datetime.datetime.fromisoformat(timestamp_str[:-1])
    # This is a timezone naive object. Twitter timestamps are always in terms of UTC.
    return timestamp_obj.replace(tzinfo=pytz.utc)


def alert():
    for i in range(3):
        print('\a', end='')
        time.sleep(0.1)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def create_engine():
    """Create SQLAlchemy engine from project and myloginpath configuration."""
    url = create_url()
    return sqlalchemy.create_engine(
        url,
        connect_args={'ssl': {'enabled': True}},
    )


def create_url():
    project_config = json.load(open('config.json', 'rb'))
    connection_config = myloginpath.parse(project_config['host'])
    url = sqlalchemy.engine.url.URL.create(
        drivername='mysql',
        username=connection_config['user'],
        password=connection_config['password'],
        host=connection_config['host'],
        port=connection_config.get('port'),
        database='dst_sentiment'
    )
    return url


def inclusive_range(start, end):
    """Range which includes end point."""
    return range(start, end + 1)
