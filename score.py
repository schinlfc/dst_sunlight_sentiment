"""For computing sentiment scoring of various tweets."""
import util

import requests
from afinn import Afinn
import datasets
from pysentimiento import create_analyzer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import multiprocessing
import argparse


class Scorer:
    def __init__(self):
        pass

    def score_tweet_df(self, tweet_df):
        """Score tweets using sentiment."""
        text = tweet_df['tweet_text'].values
        if len(text) != 0:
            # Call method with text for each tweet
            scores = self.score_tweets(text)
        else:
            # There are zero tweets to score.
            # Don't call score method.
            scores = []
        tweet_df['score'] = scores
        tweet_df['type'] = self.method
        tweet_df = tweet_df[['tweet_id', 'type', 'score']]
        return tweet_df

    def score_tweets(self, text):
        raise NotImplementedError('abstract method')


class AfinnScorer(Scorer):
    def __init__(self):
        self.method = 'afinn'
        self._analyzer = Afinn(emoticons=True)
        self.parallelism = multiprocessing.cpu_count()

    def score_tweets(self, text):
        return [self._analyzer.score(i) for i in text]


class BertScorer(Scorer):
    def __init__(self):
        self.method = 'bert'
        self._analyzer = create_analyzer(task='sentiment', lang='en')
        self.parallelism = 1
        self.local = True

    def score_tweets(self, text):
        datasets.set_progress_bar_enabled(False)
        if self.local:
            probabilities = self._analyzer.predict(text)
            # Convert probabilities of each class to sentiment score
            scores = [
                -5 * i.probas['NEG'] +
                0 * i.probas['NEU'] +
                5 * i.probas['POS']
                for i in probabilities
            ]
        else:
            text = list(text)
            url = 'http://192.168.2.242:8080/'
            scores = []
            text_returned = []
            for text_chunk in util.chunks(text, 1000):
                response = requests.post(url, json={'text': text_chunk})
                response.raise_for_status()
                response_json = response.json()
                assert isinstance(response_json['scores'], list)
                assert len(response_json['scores']) == len(text_chunk)
                scores.extend(response_json['scores'])
                text_returned.extend(response_json['text'])
            assert text == text_returned
        assert len(scores) == len(text)
        return scores


class VaderScorer(Scorer):
    def __init__(self):
        self.method = 'vader'
        self._analyzer = SentimentIntensityAnalyzer()
        self.parallelism = multiprocessing.cpu_count()

    def score_tweets(self, text):
        return [self._analyzer.polarity_scores(i)['compound'] for i in text]


scorers = {
    'vader': VaderScorer,
    'afinn': AfinnScorer,
    'bert': BertScorer,
}


def get_scorer(method):
    return scorers[method]()


def get_all_scoring_methods():
    return list(scorers.keys())


def main():
    """For testing purposes only."""
    parser = argparse.ArgumentParser()
    parser.add_argument('method')
    parser.add_argument('text')

    args = parser.parse_args()
    scorer = get_scorer(args.method)
    print(scorer.score_tweets([args.text]))


if __name__ == '__main__':
    main()
