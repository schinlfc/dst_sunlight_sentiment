#!/usr/bin/env python3
import polars as pl
import pandas as pd
import argparse
import util
import sqlalchemy
import datetime


class DataSource:
    def __init__(self, url):
        self.url = url

    def read_sql(self, name, query):
        engine = sqlalchemy.create_engine(self.url)
        with engine.connect() as con:
            print(f'Fetching {name}')
            df = pl.DataFrame(pd.read_sql(query, con=con))
        engine.dispose()
        return df

    def get_scores(self):
        score_df = self.read_sql('scores', 'select * from score')
        methods = score_df.select(pl.col('type').unique())['type'].to_list()
        score_df = score_df.pivot(index='tweet_id', columns='type', values='score')
        score_df = score_df.rename({
            method: ('score_' + method) for method in methods
        })
        return score_df

    def localize_time(self, time_col, offset_col):
        one_hour_in_microseconds = 60 * 60 * 1000 * 1000
        time_int_us = time_col.dt.epoch('us')
        time_localized = time_int_us + (offset_col * one_hour_in_microseconds).cast(int)
        return time_localized.cast(pl.Datetime)

    def format_time(self, time_col):
        """Expand time column into component parts, each one as an integer."""
        col_list = [
            time_col.dt.year().alias('legal_year'),
            time_col.dt.month().alias('legal_month'),
            time_col.dt.day().alias('legal_day'),
            time_col.dt.hour().alias('legal_hour'),
            time_col.dt.minute().alias('legal_minute'),
            time_col.dt.second().alias('legal_second'),
        ]
        return col_list

    def get_spring_fall(self, tweet_df):
        replace_dict = {
            2: 'S',
            3: 'S',
            4: 'S',
            5: 'S',
            10: 'F',
            11: 'F',
            12: 'F',
        }
        tweet_df = tweet_df.with_column(
            self.replace(tweet_df['month_number'], replace_dict, 'U').alias('spring_fall_indicator')
        )
        return tweet_df

    def replace(self, column, replace_dict, default_value=None):
        # initiate the expression with `pl`
        branch = pl

        # for every value add a `when.then`
        for from_value, to_value in replace_dict.items():
            branch = branch.when(column == from_value).then(to_value)

        # finish with an `otherwise`
        return branch.otherwise(pl.lit(default_value))

    def get_transition_indicators(self, tweet_df):
        tweet_df = tweet_df.lazy()
        cols = [
            (7, 'within_1wk_transition'),
            (14, 'within_2wk_transition'),
            (21, 'within_3wk_transition'),
            (28, 'within_4wk_transition'),
        ]
        for days_limit, col_name in cols:
            tweet_df = tweet_df.with_column(
                (pl.col('days_since_transition').abs() < days_limit).cast(pl.datatypes.Int8).alias(col_name),
            )
        return tweet_df.collect()

    def normalize_scores(self, tweet_df):
        """z-score the score columns."""
        score_cols = sorted(col for col in tweet_df.columns if col.startswith('score_'))
        # Subtract mean
        cols = [
            pl.col(col) - pl.col(col).mean()
            for col in score_cols
        ]
        tweet_df = tweet_df.with_columns(cols)

        # Divide by standard deviation
        cols = [
            pl.col(col) / pl.col(col).std()
            for col in score_cols
        ]
        tweet_df = tweet_df.with_columns(cols)
        return tweet_df

    def round_scores(self, tweet_df):
        score_cols = sorted(col for col in tweet_df.columns if col.startswith('score_'))
        cols = [
            pl.col(col).round(5)
            for col in score_cols
        ]
        tweet_df = tweet_df.with_columns(cols)
        return tweet_df

    def drop_2020_data(self, tweet_df):
        # Bounds of period to drop
        start = datetime.datetime(2020, 1, 1)
        end = datetime.datetime(2021, 1, 1)
        tweet_df = tweet_df.filter(
            # If not between 2020-1-1 and 2021-1-1, keep the row
            # Use non-inclusive upper bound
            ~(pl.col('date').is_between(start, end, include_bounds=[True, False]))
        )
        return tweet_df

    def any_nulls(self, df):
        return df.null_count().fold(lambda acc, s: acc + s)[0] != 0

    def drop_null(self, tweet_df):
        for col in tweet_df.null_count():
            name = col.name
            null_count = col[0]
            if null_count != 0:
                print(f'Column {name} contains {null_count} nulls')
        tweet_df = tweet_df.drop_nulls()
        return tweet_df

    def get_data2(self):
        tweet_df = self.read_sql('tweets', """
        select
            t.tweet_id, t.user_id, t.date,
            p.latitude, p.longitude, p.state,
            ltz.local_legal_time_offset_ci_lower as legal_time_offset_ci_lower,
            ltz.local_legal_time_offset_ci_point as legal_time_offset_ci_point,
            ltz.local_legal_time_offset_ci_upper as legal_time_offset_ci_upper,
            ltz.is_dst, ltz.days_since_transition, ltz.timezone_experiences_dst,
            ltz.most_probable_tz
        from
            tweet t
        left join
            place p
        on
            p.place_id = t.place_id
        left join
            tweet_legal_tz ltz
        on
            ltz.tweet_id = t.tweet_id
        where
            p.state is not null
        """)
        tweet_df = tweet_df.lazy().with_columns([
            pl.col('date').str.strptime(pl.Datetime, '%Y-%m-%dT%H:%M:%S.000Z'),
        ]).collect()
        tweet_df = tweet_df.with_column(
            self.localize_time(tweet_df['date'], tweet_df['legal_time_offset_ci_point']).alias('legal_datetime')
        )
        tweet_df = tweet_df.with_columns([
            tweet_df['legal_datetime'].dt.weekday().alias('legal_day_of_week'),
            tweet_df['legal_datetime'].dt.month().alias('month_number'),
        ])
        tweet_df = tweet_df.with_columns(
            self.format_time(tweet_df['legal_datetime'])
        )
        # Add scores
        score_df = self.get_scores()
        tweet_df = tweet_df.join(score_df, on=['tweet_id'], how='left')

        # Get spring/fall indicator
        tweet_df = self.get_spring_fall(tweet_df)

        # Get transition indicators
        tweet_df = self.get_transition_indicators(tweet_df)

        # Filter 2020 data
        # print('Dropping 2020')
        # tweet_df = self.drop_2020_data(tweet_df)

        # Standardize scores
        print('Normalizing scores')
        tweet_df = self.normalize_scores(tweet_df)

        print('Rounding scores')
        tweet_df = self.round_scores(tweet_df)

        # Do final formatting for output, including dropping unused cols
        # and re-ordering the columns.
        drop_cols = [
            'date',
            'month_number',
        ]
        tweet_df = tweet_df.drop(drop_cols)
        sentiment_cols = sorted(col for col in tweet_df.columns if col.startswith('score_'))
        col_order = [
            # Tweet info
            'tweet_id',
            'user_id',
            # Geographic information
            'latitude',
            'longitude',
            'state',
            # Legal time info
            'most_probable_tz',
            'legal_datetime',
            'legal_year',
            'legal_month',
            'legal_day',
            'legal_hour',
            'legal_minute',
            'legal_second',
            'legal_day_of_week',
            'legal_time_offset_ci_lower',
            'legal_time_offset_ci_point',
            'legal_time_offset_ci_upper',
            'days_since_transition',
            'is_dst',
            'timezone_experiences_dst',
            # Sentiment
            *sentiment_cols,
            # Treatment intensity information
            'within_1wk_transition',
            'within_2wk_transition',
            'within_3wk_transition',
            'within_4wk_transition',
            # Spring/fall indicator
            'spring_fall_indicator',
        ]
        assert all(col in col_order for col in tweet_df.columns), \
            f'missing column from col_order, cols: {tweet_df.columns}'
        if self.any_nulls(tweet_df):
            tweet_df = self.drop_null(tweet_df)
        # Reorder columns
        tweet_df = tweet_df[col_order]
        return tweet_df


def parse_args():
    parser = argparse.ArgumentParser(
        prog='DST data export',
        description='Export CSV for tweets gathered so far'
    )
    parser.add_argument(
        'filename',
    )

    return parser.parse_args()


def main():
    args = parse_args()
    url = str(util.create_url())
    df = DataSource(url).get_data2()
    # print(df)
    print(f'{len(df)} rows written')
    df.write_csv(args.filename)


if __name__ == '__main__':
    main()

# con.close()
