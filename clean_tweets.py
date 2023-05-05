#!/usr/bin/env python3
import util
import score

import json
import argparse
import itertools
import multiprocessing
import state_boundaries
import timezone_boundaries
from tqdm import tqdm
import pandas as pd
import sqlalchemy
from cachetools import cached


@util.listify
def transform_objects_by_path(obj_list, path_dict):
    try:
        for obj in obj_list:
            obj_transformed = {}
            for path_name, path_path in path_dict.items():
                current = obj
                for path_component in path_path.split('.'):
                    if path_component.isdigit():
                        # If composed solely of digits, interpret as list index
                        current = current[int(path_component)]
                    else:
                        current = current[path_component]
                obj_transformed[path_name] = current
            yield obj_transformed
    except KeyError:
        raise Exception(f'Unable to match {path_path} to {obj}')
    return obj


def create_tables(con):
    tables = [
        """CREATE TABLE tweet
                       (tweet_id varchar(19) UNIQUE, date varchar(24), user_id varchar(19),
                        tweet_text text character set utf8mb4 collate utf8mb4_unicode_ci,
                        place_id varchar(16))""",
        """CREATE TABLE user
                       (user_id varchar(19) UNIQUE, user_name text)""",
        """CREATE TABLE place
                       (place_id varchar(16) UNIQUE, state varchar(2),
                        latitude double, longitude double,
                        minx double, miny double,
                        maxx double, maxy double)""",
        """CREATE TABLE score
                       (tweet_id varchar(19), type varchar(8), score double,
                       PRIMARY KEY (tweet_id, type))""",
        """CREATE TABLE tweet_legal_tz
                       (tweet_id varchar(19) UNIQUE, most_probable_tz varchar(40),
                       local_legal_time_offset_ci_lower double,
                       local_legal_time_offset_ci_point double,
                       local_legal_time_offset_ci_upper double,
                       is_dst double,
                       timezone_experiences_dst double,
                       days_since_transition double)""",
        """CREATE TABLE tweet_time_summary
                       (date_day varchar(10) UNIQUE, cnt int)""",
    ]
    # Create table
    for table_sql in tables:
        try:
            con.execute(table_sql)
        except sqlalchemy.exc.OperationalError:
            # Table already exists, ignore
            pass


def load_nl_deliminted_json_file(filename, chunk_size):
    """Reads a file where each line is a separate JSON object.
    Reads the file into chunks of chunk_size long and yields
    it lazily as an iterator."""
    with open(filename, 'rt') as f:
        line_num = 1
        while True:
            objects = []
            for i in range(chunk_size):
                try:
                    # Read one additional line, parse as JSON, and add
                    # to objects list
                    try:
                        objects.append(json.loads(next(f)))
                        line_num += 1
                    except json.decoder.JSONDecodeError as e:
                        raise Exception(f'Error on line {line_num}') from e
                except StopIteration:
                    # End of file. Exit the for loop.
                    # Still yield the objects list if we got any.
                    break
            if len(objects) > 0:
                if chunk_size == 1:
                    yield objects[0]
                else:
                    yield objects
            else:
                return


def read_tweets():
    return load_nl_deliminted_json_file('tweets.json', 10000)


def filter_places_from_tweets(tweets, chunk_size=500):
    place_existing = set()
    partial_chunk = []
    for tweet_chunk in tweets:
        assert isinstance(tweet_chunk, list)
        place_chunk = [
            tweet['place']
            for tweet in tweet_chunk
            if 'place' in tweet
        ]
        place_chunk = [
            place
            for place in place_chunk
            # This can technically include the same place twice
            # in the same chunk, but w/e
            if place['id'] not in place_existing
        ]
        place_existing.update(
            place['id']
            for place in place_chunk
        )
        partial_chunk.extend(place_chunk)
        # If the partial chunk is big enough, emit it
        if len(partial_chunk) > chunk_size:
            yield partial_chunk
            partial_chunk = []
    # Yield any leftover chunks
    if len(partial_chunk) > 0:
        yield partial_chunk


def read_places():
    places_only = load_nl_deliminted_json_file('places.json', 500)
    tweets = load_nl_deliminted_json_file('tweets.json', 500)
    places_from_tweets = filter_places_from_tweets(tweets)
    return itertools.chain(places_only, places_from_tweets)


@cached(cache={}, key=lambda con: 0)
def get_tables(con):
    return [x[0] for x in con.execute('show tables').fetchall()]


def insert_dataframe(df, table, pk, con):
    """Insert dataframe `df` into table `table`, ignoring rows which already have a match
    in the `pk` column. If `pk` is None, then insert all rows."""
    # Make sure the table exists
    assert table in get_tables(con)
    if pk is not None:
        # For each row, check if the primary key alreay exists
        pk_list = df[pk].to_list()
        pk_query = f"{pk} in ({', '.join(map(repr, pk_list))})"
        num_present = pd.read_sql_query(f'select count(*) from {table} where {pk_query}', con=con).iloc[0, 0]
        if num_present == len(df):
            # All rows which could be inserted already have been
            return
        already_present = pd.read_sql_query(f'select {pk} from {table} where {pk_query}', con=con)
        # Filter rows already present
        df = df[~df[pk].isin(already_present[pk])]
    if len(df) > 0:
        df.to_sql(table, if_exists='append', index=False, con=con)


def insert_tweets(tweets, con):
    tweets = pd.json_normalize(tweets)
    # Check that columns are present
    tweets = tweets.rename(columns={
        'tweet.id': 'tweet_id',
        'tweet.created_at': 'date',
        'tweet.author_id': 'user_id',
        'tweet.text': 'tweet_text',
        'tweet.geo.place_id': 'place_id',
    })
    tweets = tweets[['tweet_id', 'date', 'user_id', 'tweet_text', 'place_id']]
    insert_dataframe(tweets, 'tweet', 'tweet_id', con)


def infer_centroid(place):
    bounding_box = util.convert_twitter_bbox_to_polygon(place['geo']['bbox'])
    cent = bounding_box.centroid
    place['centroid'] = [cent.x, cent.y]


def infer_bbox(place):
    bounding_box = util.get_bbox_from_place(place)
    if 'geo' not in place:
        place['geo'] = {}
    place['geo']['bbox'] = bounding_box.bounds


@util.listify
def geocode_places(places):
    for place in places:
        state = state_boundaries.geocode_place(place)
        if state is None:
            # Only include place if it geocodes somewhere
            continue
        place['state'] = state
        if 'centroid' not in place and 'geo' in place:
            infer_centroid(place)
        if 'geo' not in place:
            infer_bbox(place)
        yield place


def insert_places(places, con):
    data_columns = {
        'place_id': 'id',
        'state': 'state',
        'latitude': 'centroid.1',
        'longitude': 'centroid.0',
        'minx': 'geo.bbox.0',
        'miny': 'geo.bbox.1',
        'maxx': 'geo.bbox.2',
        'maxy': 'geo.bbox.3',
    }
    places = transform_objects_by_path(places, data_columns)
    places = pd.DataFrame(places)
    places = places.drop_duplicates(subset=['place_id'])
    insert_dataframe(places, 'place', 'place_id', con)


def select_unscored_tweets(method, con):
    df_generator = pd.read_sql_query(
        f"""
        select t.tweet_id, t.tweet_text from
            tweet t
        left join
            score s
        on
            t.tweet_id = s.tweet_id and
            s.type = '{method}'
        where
            s.tweet_id is null;""",
        con=con,
        chunksize=100000,
    )
    return df_generator


def count_unscored_tweets(method, con):
    count = pd.read_sql_query(
        f"""
        select count(*) from
            tweet t
        left join
            score s
        on
            t.tweet_id = s.tweet_id and
            s.type = '{method}'
        where
            s.tweet_id is null;""",
        con=con,
    ).iloc[0, 0]
    return count


def insert_scores(df, con):
    insert_dataframe(df, 'score', pk=None, con=con)


def select_tweets_without_timezones(con):
    df_iter = pd.read_sql_query(
        """
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
            tz.tweet_id is null and
            p.minx is not null
        order by
            t.place_id
        """,
        con=con,
        chunksize=10000,
    )
    for df in df_iter:
        if len(df) > 0:
            yield df


def select_tweets_without_timezones_incremental(tweet_ids, con):
    query = f"""
        select
            t.tweet_id, t.date, t.place_id, p.minx, p.miny, p.maxx, p.maxy
        from
            tweet t
        left join
            place p
        on
            t.place_id = p.place_id
        where
            p.minx is not null and
            t.tweet_id in ({','.join(map(repr, tweet_ids))})
        ;
        """
    return pd.read_sql_query(
        query,
        con=con,
    )


def insert_timezones(df, con):
    insert_dataframe(df, 'tweet_legal_tz', 'tweet_id', con)


def load_tweets_from_file(progbar_size, con):
    print('Loading tweets')
    with tqdm(total=progbar_size) as prog:
        for tweets in read_tweets():
            insert_tweets(tweets, con)
            prog.update(len(tweets))


def load_places_from_file(progbar_size, con):
    print('Loading places')
    with tqdm(total=progbar_size) as prog:
        for places in read_places():
            places = geocode_places(places)
            insert_places(places, con)
            prog.update(len(places))


def load_scores_incremental(score_df, con):
    for method in score.get_all_scoring_methods():
        scorer = score.get_scorer(method)
        insert_scores(scorer.score_tweet_df(score_df), con)


def load_scores_all(con_read, con_write):
    print('Scoring tweets')
    for method in score.get_all_scoring_methods():
        scorer = score.get_scorer(method)
        number_tweets = count_unscored_tweets(method, con_read)
        unscored_iter = select_unscored_tweets(method, con_read)
        print(f'Scoring tweets with {method}')
        with tqdm(total=number_tweets) as prog:
            num_processes = scorer.parallelism
            if num_processes == 1:
                for score_df in map(scorer.score_tweet_df, unscored_iter):
                    insert_scores(score_df, con_write)
                    prog.update(len(score_df))
            else:
                with multiprocessing.Pool(num_processes) as p:
                    for score_df in p.imap(scorer.score_tweet_df, unscored_iter):
                        insert_scores(score_df, con_write)
                        prog.update(len(score_df))


def load_timezones_all(con):
    print('Finding timezones')
    tweet_count = util.table_row_count(con, 'tweet')
    tz_count = util.table_row_count(con, 'tweet_legal_tz')
    total = tweet_count - tz_count
    df_iter = select_tweets_without_timezones(con)
    with tqdm(total=total) as prog:
        num_procs = multiprocessing.cpu_count()
        with multiprocessing.Pool(num_procs) as p:
            for tz_chunk in p.imap(timezone_boundaries.get_tz_for_tweets, df_iter):
                insert_timezones(tz_chunk, con)
                prog.update(len(tz_chunk))


def update_time_summary(con):
    query = """
        select
            left(date, 10) as date_day,
            count(*) as cnt
        from
            tweet
        group by
            date_day;
        """
    print('Analyzing what day most tweets come from')
    tweet_time_summary = pd.read_sql_query(query, con=con)
    con.execute('truncate table tweet_time_summary')
    tweet_time_summary.to_sql('tweet_time_summary', if_exists='append', index=False, con=con)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--enable-all',
        action='store_true'
    )
    parser.add_argument(
        '--enable-tables',
        action='store_true'
    )
    parser.add_argument(
        '--enable-tweets',
        action='store_true'
    )
    parser.add_argument(
        '--enable-places',
        action='store_true'
    )
    parser.add_argument(
        '--enable-scores',
        action='store_true'
    )
    parser.add_argument(
        '--enable-tz',
        action='store_true'
    )
    parser.add_argument(
        '--enable-time-summary',
        action='store_true'
    )

    return parser.parse_args()


def clean_tweets_incremental(tweets, places, con):
    # Tweets
    insert_tweets(tweets, con)

    # Places
    places = geocode_places(places)
    insert_places(places, con)

    # Scores
    ids = [tweet['tweet']['id'] for tweet in tweets]
    text = [tweet['tweet']['text'] for tweet in tweets]
    score_df = pd.DataFrame({'tweet_id': ids, 'tweet_text': text})
    load_scores_incremental(score_df, con)

    # Timezones
    tz_df = select_tweets_without_timezones_incremental(ids, con)
    tz_df = timezone_boundaries.get_tz_for_tweets(tz_df)
    insert_timezones(tz_df, con)


def main():
    engine = util.create_engine()

    args = parse_args()

    enable_all = args.enable_all
    if enable_all:
        enable_tables = True
        enable_tweets = True
        enable_places = True
        enable_scores = True
        enable_tz = True
        enable_time_summary = True
    else:
        enable_tables = args.enable_tables
        enable_tweets = args.enable_tweets
        enable_places = args.enable_places
        enable_scores = args.enable_scores
        enable_tz = args.enable_tz
        enable_time_summary = args.enable_time_summary

    with engine.connect() as con:
        if enable_tables:
            create_tables(con)
        if enable_places or enable_tweets:
            number_tweets = util.fast_line_count('tweets.json')
            number_places = int(number_tweets * 8.9e-3)  # Progbar size guess
        if enable_tweets:
            load_tweets_from_file(number_tweets, con)
        if enable_places:
            load_places_from_file(number_places, con)
        if enable_scores:
            with engine.connect() as con2:
                load_scores_all(con2, con)
        if enable_tz:
            load_timezones_all(con)
        if enable_time_summary:
            update_time_summary(con)


if __name__ == '__main__':
    main()
