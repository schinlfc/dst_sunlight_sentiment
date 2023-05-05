import util
import state_boundaries
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import box
import datetime
import pytz
import iteround
import cachetools
import functools
import bisect
from numbers import Number
import sys


@util.gdf_file_cache('shape/timezones.shp')
def get_timezone_shapefile():
    filename = util.script_relative('shape/timezones.shapefile.zip')
    url = 'https://github.com/evansiroky/timezone-boundary-builder/releases/' \
        'download/2021c/timezones.shapefile.zip'
    util.maybe_download_file(url, filename)
    timezones = gpd.read_file('zip://' + filename)
    timezones = timezones.set_index('tzid').sort_index()
    # Filter timezone file. Get just the United States.
    all_united_states = state_boundaries.get_states().unary_union
    timezones_area = timezones.to_crs('+proj=cea').area
    timezones_area_us = timezones.intersection(all_united_states).to_crs('+proj=cea').area
    percent_in_us = timezones_area_us / timezones_area * 100

    # Drop polygons which have less than 1% of their area in the US
    # Note: this is preferable to a straight intersection operation, because that
    # leaves us with lots of GeometryCollections which can't be saved back into
    # a shapefile.
    timezones = timezones[percent_in_us >= 1]

    # Do a final pass, removing parts of the timezone not in the US
    timezones = timezones.intersection(all_united_states)
    return timezones


def lookup_by_geospatial(poly, recurse_limit=1):
    if poly.area == 0:
        poly = poly.buffer(1e-3)
    timezones = get_timezone_shapefile()
    intersections = timezones.intersects(poly)
    num_intersections = intersections.sum()
    if num_intersections == 0:
        if recurse_limit > 0:
            # Zero intersections? Try again, but a larger search area
            poly = poly.buffer(1e-2)
            return lookup_by_geospatial(poly, recurse_limit - 1)
        else:
            # Hit recursion limit, stop searching
            return None
    elif num_intersections == 1:
        tz = timezones[intersections].index[0]
        return pd.Series({tz: 100.0})
    intersecting_timezones = timezones[intersections]
    tz_areas = util.get_percentage_overlap(intersecting_timezones, poly, cea=False)
    return tz_areas


geospatial_cache = cachetools.Cache(128)


def lookup_by_geospatial_cached(bbox, place_id):
    try:
        return geospatial_cache[place_id]
    except KeyError:
        pass
    assert len(bbox) == 4
    poly = box(*bbox)
    areas = lookup_by_geospatial(poly)
    assert areas is not None, f"geospatial search failed for {place_id}"
    geospatial_cache[place_id] = areas
    return areas


@functools.lru_cache(128)
def lookup_timezone_by_name(tz_name):
    return pytz.timezone(tz_name)


@functools.lru_cache(128)
def get_dst_changeover_for_year(year, spring=True):
    # Look up DST transitions from this timezone
    tz = lookup_timezone_by_name('America/Denver')
    # Set time_naive to a time after the transition of interest
    if spring:
        time_naive = datetime.datetime(year, 6, 1)
    else:
        time_naive = datetime.datetime(year, 12, 1)
    # Using bisect_right(), perform a binary search for the transition
    idx = max(0, bisect.bisect_right(tz._utc_transition_times, time_naive) - 1)
    transition = tz._utc_transition_times[idx]
    # Check that the transition is the same year as the changeover we're looking for
    assert transition.year == year, f'DST changeover too old, have {transition=}'
    # Look up DST offset for transition, current DST transition, check that this
    # is actually a 1 hr DST transition
    dst_offset = tz._transition_info[idx][1]
    prev_dst_offset = tz._transition_info[idx - 1][1]
    assert abs((dst_offset - prev_dst_offset).total_seconds()) == 3600, \
        f'Wrong DST change, have {(dst_offset - prev_dst_offset).total_seconds()=}'
    transition = transition.replace(tzinfo=lookup_timezone_by_name('UTC'))
    transition = transition.astimezone(tz).replace(tzinfo=None)
    return transition


@functools.lru_cache(128)
def get_timezone_experiences_dst(tz, year):
    time = datetime.datetime(year, 12, 31)
    idx = max(0, bisect.bisect_right(tz._utc_transition_times, time) - 1)
    transition = tz._utc_transition_times[idx]
    return transition.year == year


def get_transition_for_datetime(time):
    year = time.year
    spring = is_spring(time)
    return get_dst_changeover_for_year(year, spring)


def is_spring(time):
    return time.month <= 6


def localize_time_by_each_tz(time, tz_areas):
    def get_tz_info(tz):
        seconds_in_day = 24 * 60 * 60
        tz = lookup_timezone_by_name(tz)
        time_conv = time.astimezone(tz)
        seconds_in_hour = 60 * 60
        offset_hrs = time_conv.utcoffset().total_seconds() / seconds_in_hour

        transition = get_transition_for_datetime(time)
        time_naive = time_conv.replace(tzinfo=None)
        transition_offset_days = (time_naive - transition).total_seconds() / seconds_in_day
        before_transition = transition_offset_days <= 0
        if is_spring(time):
            is_dst = int(not before_transition)
        else:
            is_dst = int(before_transition)

        timezone_experiences_dst = get_timezone_experiences_dst(tz, time_naive.year)

        assert -35 <= transition_offset_days <= 35, f'{transition_offset_days=} out of range'
        assert -24 <= offset_hrs <= 24
        assert is_dst in [0, 1]
        assert timezone_experiences_dst in [0, 1]

        return offset_hrs, is_dst, transition_offset_days, timezone_experiences_dst

    df = pd.DataFrame(tz_areas, columns=['area'])
    timezones = df.index.to_series()
    df[[
        'offset',
        'dst',
        'days_since_transition',
        'timezone_experiences_dst',
    ]] = timezones.apply(get_tz_info).to_list()
    if len(df) == 1:
        # Only one timezone match
        # Fast path
        point_estimate = df['offset'].iloc[0]
        lower = point_estimate
        upper = point_estimate
        is_dst = df['dst'].iloc[0]
        days_since_transition = df['days_since_transition'].iloc[0]
        timezone_experiences_dst = df['timezone_experiences_dst'].iloc[0]
    else:
        # Slow path
        # Round area so that the total sum is preserved
        area = pd.Series(iteround.saferound(df['area'].to_dict(), places=0, strategy='difference'))
        # Constant controlling how many times to duplicate each percentage
        # of area. Higher values make a better approximation of statistical
        # aggregates, but at the cost of compute time
        number_copies_per_percent = 1
        area = (area * number_copies_per_percent).round().astype(int)
        df['area'] = area
        if df['area'].sum() != 100 * number_copies_per_percent:
            breakpoint()
        # Repeat row as many times as appears in area
        df = df.loc[df.index.repeat(df['area'])]
        assert len(df) == 100 * number_copies_per_percent, \
            f'df had {len(df)} rows, should be {100 * number_copies_per_percent}'
        offset = df['offset'].values
        lower, upper = np.quantile(offset, q=[0.025, 0.975])
        point_estimate = offset.mean()
        # Ensure that the point estimate always lies within the range [lower, upper]
        point_estimate = min(max(lower, point_estimate), upper)
        assert lower <= point_estimate <= upper
        is_dst = df['dst'].mean()
        # Get the most common number of days since transition
        days_since_transition = df['days_since_transition'].mode().iloc[0]
        timezone_experiences_dst = df['timezone_experiences_dst'].mean()
    assert isinstance(lower, Number)
    assert isinstance(point_estimate, Number)
    assert isinstance(upper, Number)
    assert isinstance(is_dst, Number), f'is_dst has type {type(is_dst)}'
    assert isinstance(days_since_transition, Number)
    assert isinstance(timezone_experiences_dst, Number)
    return lower, point_estimate, upper, is_dst, days_since_transition, timezone_experiences_dst


def get_tz_for_tweets(tweets):
    assert isinstance(tweets, pd.DataFrame)
    assert len(tweets) > 0, "Can't process zero-len tweet dataframe"

    def get_offset_summary(row):
        try:
            bbox = tuple(row[['minx', 'miny', 'maxx', 'maxy']])
            areas = lookup_by_geospatial_cached(bbox, row['place_id'])
            most_probable_tz = areas.index[0]
            timestamp = util.parse_twitter_timestamp(row['date'])
            lower, point_estimate, upper, is_dst, days_since_transition, timezone_experiences_dst = \
                localize_time_by_each_tz(timestamp, areas)
            row['most_probable_tz'] = most_probable_tz
            row['local_legal_time_offset_ci_lower'] = lower
            row['local_legal_time_offset_ci_point'] = point_estimate
            row['local_legal_time_offset_ci_upper'] = upper
            row['is_dst'] = is_dst
            row['days_since_transition'] = days_since_transition
            row['timezone_experiences_dst'] = timezone_experiences_dst
        except Exception as e:
            tweet_id = row['tweet_id']
            raise Exception(f'Error encountered while processing {tweet_id=}') from e
        return row

    tweets = tweets.apply(get_offset_summary, axis=1)

    tweets = tweets[[
        'tweet_id',
        'most_probable_tz',
        'local_legal_time_offset_ci_lower',
        'local_legal_time_offset_ci_point',
        'local_legal_time_offset_ci_upper',
        'is_dst',
        'days_since_transition',
        'timezone_experiences_dst',
    ]]
    return tweets


def test_ri():
    engine = util.create_engine()
    id = sys.argv[1]
    with engine.connect() as con:
        tz = pd.read_sql_query(
            f"""
            select
                t.tweet_id, t.date, t.place_id, p.minx, p.miny, p.maxx, p.maxy
            from
                tweet t
            left join
                tweet_legal_tz tz
            on
                t.tweet_id = tz.tweet_id
            left join
                place p
            on
                t.place_id = p.place_id
            where
                p.minx is not null and
                t.tweet_id = '{id}';
            """,
            con=con,
        )
    print(tz)
    tz = get_tz_for_tweets(tz)
    print(tz.to_string())
    con.close()


if __name__ == '__main__':
    test_ri()
