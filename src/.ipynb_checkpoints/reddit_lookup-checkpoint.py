import json
import os
import time
import praw
import datetime
from datetime import datetime
from praw.models import MoreComments

import dotenv
import pandas as pd
import requests
import yaml
from dateutil import parser

from generic_db import GenericDBOperations

pd.set_option('display.expand_frame_repr', False)


class RedditLookup:
    def __init__(self):
        self._load_env()
        self._load_reddit()
        self._load_search_keywords()
        self.generic_db = GenericDBOperations()
        self._register_subreddits()
        self._load_subreddits()
        self.results_so_far = 0

    @staticmethod
    def _load_env():
        dotenv_file = os.path.join(os.getcwd(), '.env')
        if os.path.isfile(dotenv_file):
            dotenv.load_dotenv(dotenv_file)

    def _load_credentials(self):
        return {
            'client_id': os.environ['client_id'],
            'client_secret': os.environ['client_secret'],
            'username': os.environ['username'],
            'password': os.environ['password'],
            'user_agent': os.environ['user_agent'],
        }

    def _load_reddit(self):
        creds = self._load_credentials()
        self.reddit = praw.Reddit(**creds)
        assert isinstance(self.reddit, praw.Reddit)

    def _load_search_keywords(self):
        with open('config/keywords.yml') as config_stream:
            self.search_keywords = yaml.full_load(config_stream)['keywords']
        print('Search Keywords:')
        print(self.search_keywords)

    @staticmethod
    def _create_subreddit_df(subreddit_instance: praw.models.Subreddit):
        subreddit_df = {}
        try:
            subreddit_df['subreddit_id'] = [subreddit_instance.fullname]
        except:
            try:
                subreddit_df['subreddit_id'] = [subreddit_instance.name]
            except:
                subreddit_df['subreddit_id'] = [subreddit_instance.id]

        try:
            subreddit_df['display_name'] = [subreddit_instance.display_name]
        except:
            subreddit_df['display_name'] = [None]

        try:
            subreddit_df['description'] = [subreddit_instance.public_description]
        except:
            subreddit_df['description'] = [None]

        try:
            subreddit_df['subscribers'] = [subreddit_instance.subscribers]
        except:
            subreddit_df['subscribers'] = [None]

        try:
            subreddit_df['over18'] = [subreddit_instance.over18]
        except:
            subreddit_df['over18'] = [None]

        try:
            subreddit_df['created_utc'] = [subreddit_instance.created_utc]
        except:
            subreddit_df['created_utc'] = [None]

        try:
            created_at = datetime.fromtimestamp(subreddit_instance.created_utc)
            subreddit_df['created_at'] = [created_at]
        except:
            subreddit_df['created_at'] = [None]

        subreddit_df = pd.DataFrame.from_dict(data=subreddit_df)
        return subreddit_df

    def _register_subreddits(self):
        with open('config/subreddits.yml') as config_stream:
            subreddits = yaml.full_load(config_stream)['subreddits']

        for sub_name in subreddits:
            fetched_sub = self.generic_db.lookup_table(table='subreddits', fetch_cols=['subreddit_id'],
                                                       lookup_cols=['display_name'], lookup_values=[sub_name],
                                                       fetch_one=True)
            # If the subreddit is not already registered, then register it to DB
            if self.generic_db.check_db_result_sanity(fetched_sub):
                subreddit_instance = self.reddit.subreddit(sub_name)
                assert isinstance(subreddit_instance, praw.models.Subreddit)
                subreddit_to_insert = self._create_subreddit_df(subreddit_instance)
                self.generic_db.insert_into_table(table_name='subreddits', data=subreddit_to_insert,
                                                  id_col='subreddit_id')
            else:
                print(f'Subreddit {sub_name} already registered.')

    def _load_subreddits(self):
        subreddit_cols = self.generic_db.get_columns_of_table(table_name='subreddits')
        all_subs_q = f'SELECT * FROM subreddits;'
        all_subs = self.generic_db.execute_query(query=all_subs_q, fetch_all=True)
        if self.generic_db.check_db_result_sanity(all_subs):
            return None
        self.all_subs = pd.DataFrame(data=all_subs, columns=subreddit_cols)

        print('All subreddits:')
        print(all_subs)

    def process_users(self, users, disorder):
        user_data = {
            'id': [],
            'location': [],
            'created_at': [],
            'verified': [],
            'name': [],
            'username': [],
            'description': []
        }

        for user in users:
            for col in ['id', 'location', 'verified', 'name', 'username', 'description']:
                try:
                    col_val = user[col]

                    # try:
                    #     col_val = col_val.replace('\'', '')
                    # except:
                    #     col_val = user[col]
                    user_data[col].append(col_val)
                except:
                    user_data[col].append(None)

            created_at = user['created_at']
            created_at = parser.parse(created_at)
            user_data['created_at'].append(created_at)

        user_data['disorder'] = [disorder] * len(user_data['id'])
        return pd.DataFrame.from_dict(data=user_data)

    def process_tweets(self, tweets, disorder, tweet_type):
        tweet_data = {
            'id': [],
            'tweet_type': [],
            'text': [],
            'referenced_tweet_type': [],
            'referenced_tweet_id': [],
            'created_at': [],
            'lang': [],
            'retweet_count': [],
            'reply_count': [],
            'like_count': [],
            'quote_count': [],
            'source': [],
            'disorder': [],
            'author_id': [],
        }
        for tweet in tweets:
            for col in ['id', 'text', 'lang', 'source', 'author_id']:
                try:
                    col_val = tweet[col]
                    # try:
                    #     col_val = col_val.replace('\'', '')
                    # except:
                    #     col_val = tweet[col]
                    tweet_data[col].append(col_val)
                except:
                    tweet_data[col].append(None)

            created_at = tweet['created_at']
            created_at = parser.parse(created_at)
            tweet_data['created_at'].append(created_at)

            if 'referenced_tweets' in tweet:
                tweet_data['referenced_tweet_type'].append(tweet['referenced_tweets'][0]['type'])
                tweet_data['referenced_tweet_id'].append(tweet['referenced_tweets'][0]['id'])
            else:
                tweet_data['referenced_tweet_type'].append(None)
                tweet_data['referenced_tweet_id'].append(None)

            for col in ['retweet_count', 'reply_count', 'like_count', 'quote_count']:
                col_val = tweet['public_metrics'][col]
                tweet_data[col].append(col_val)

        tweet_data['disorder'] = [disorder] * len(tweet_data['author_id'])
        tweet_data['tweet_type'] = [tweet_type] * len(tweet_data['author_id'])

        return pd.DataFrame.from_dict(data=tweet_data)

    def parse_search_results_and_save_tweets_authors(self, response, disorder):
        # resp = json.loads(response.text)
        resp = json.loads(response)

        try:
            _ = resp['includes']['users']
            _ = resp['data']
        except:
            print(f'{disorder}: Empty Response. ', response)
            self.results_so_far = 0
            return None

        # An array of users (results of search)
        raw_users = resp['includes']['users']
        processed_users = self.process_users(raw_users, disorder)
        # print(processed_users)
        # print('\n')

        self.results_so_far += processed_users.shape[0]

        # TODO: We shouldn't continue with tweet extracts, this is just the search step.
        # TODO: Later for timeline extraction, we do the full thing

        # TODO: NOPE! WE CAN'T IGNORE ENTITIES, HASHTAGS, ETC. WE MUST INSERT THEM NOW OR THEY'RE LOST

        # An array of tweets (results of search)
        raw_tweets = resp['data']
        processed_tweets = self.process_tweets(raw_tweets, disorder, tweet_type='diagnose')
        # print(processed_tweets)
        # print('\n')

        # # URLs are extracted from tweets (the json version of tweets)
        # extracted_urls, extracted_musics = self.extract_urls_and_musics_from_tweets(raw_tweets, disorder)
        # # print(extracted_urls)
        # # print('\n')
        # # print(extracted_musics)
        # # print('\n')

        # # # Annotations are extracted from tweets (the json version of tweets)
        # extracted_annotations = self.extract_annotations_from_tweets(raw_tweets)
        # # print(extracted_annotations)
        # # print('\n')

        # # # Hashtags are extracted from tweets (the json version of tweets)
        # extracted_hashtags = self.extract_hashtags_from_tweets(raw_tweets)
        # # print(extracted_hashtags)
        # # print('\n')

        self.generic_db.insert_into_table(table_name='authors', data=processed_users, id_col='id')
        self.generic_db.insert_into_table(table_name='tweets', data=processed_tweets, id_col='id')
        # self.generic_db.insert_into_table(table_name='urls', data=extracted_urls)
        # self.generic_db.insert_into_table(table_name='musics', data=extracted_musics)
        # self.generic_db.insert_into_table(table_name='annotations', data=extracted_annotations)
        # self.generic_db.insert_into_table(table_name='hashtags', data=extracted_hashtags)

        try:
            next_token = resp['meta']['next_token']
        except:
            print(f'{disorder}: no more results. ', self.results_so_far)
            self.results_so_far = 0
            return None
        return next_token

    @staticmethod
    def search_subreddit(query: str, subreddit_instance: praw.models.Subreddit, time_filter='year'):
        submissions_generator = subreddit_instance.search(query=query, time_filter=time_filter)
        submissions = []

        for s in submissions_generator:
            if not isinstance(s, praw.models.Submission):
                print('ERROR: search result not an instance of praw.models.Submission')
                print(query)
                print(subreddit_instance.display_name)
                continue
            submissions.append(s)

        return submissions

    @staticmethod
    def get_reddit_model_id(reddit_model: praw.models.Submission | praw.models.Redditor | praw.models.Comment):
        try:
            reddit_model_id = reddit_model.fullname
        except:
            try:
                reddit_model_id = reddit_model.name
            except:
                reddit_model_id = reddit_model.id
        return reddit_model_id

    def _create_submissions_df(self, submissions, authors, keywords, has_exact_keyword, subreddit_id):
        submissions_df = {
            'submission_id': [],
            'author': authors,
            'subreddit': [subreddit_id] * len(submissions),
            'keyword': keywords,
            'has_exact_keyword': has_exact_keyword,
            'title': [],
            'score': [],
            'selftext': [],
            'upvote_ratio': [],
            'num_comments': [],
            'url': [],
            'permalink': [],
            'author_flair_text': [],
            'link_flair_text': [],
            'distinguished': [],
            'is_self': [],
            'locked': [],
            'over_18': [],
            'created_at': [],
            'created_utc': [],
        }
        for submission in submissions:
            try:
                submission_id = self.get_reddit_model_id(reddit_model=submission)
                submissions_df['submission_id'].append(submission_id)
            except:
                print('THIS SHOULD NOT HAPPEN 1')
                submissions_df['submission_id'].append(None)
            try:
                submissions_df['title'].append(submission.title)
            except:
                submissions_df['title'].append(None)
            try:
                submissions_df['score'].append(submission.score)
            except:
                submissions_df['score'].append(None)
            try:
                submissions_df['selftext'].append(submission.selftext)
            except:
                submissions_df['selftext'].append(None)
            try:
                submissions_df['upvote_ratio'].append(submission.upvote_ratio)
            except:
                submissions_df['upvote_ratio'].append(None)
            try:
                submissions_df['num_comments'].append(submission.num_comments)
            except:
                submissions_df['num_comments'].append(None)
            try:
                submissions_df['url'].append(submission.url)
            except:
                submissions_df['url'].append(None)
            try:
                submissions_df['permalink'].append(submission.permalink)
            except:
                submissions_df['permalink'].append(None)
            try:
                submissions_df['author_flair_text'].append(submission.author_flair_text)
            except:
                submissions_df['author_flair_text'].append(None)
            try:
                submissions_df['link_flair_text'].append(submission.link_flair_text)
            except:
                submissions_df['link_flair_text'].append(None)
            try:
                submissions_df['distinguished'].append(submission.distinguished)
            except:
                submissions_df['distinguished'].append(None)
            try:
                submissions_df['is_self'].append(submission.is_self)
            except:
                submissions_df['is_self'].append(None)
            try:
                submissions_df['locked'].append(submission.locked)
            except:
                submissions_df['locked'].append(None)
            try:
                submissions_df['over_18'].append(submission.over_18)
            except:
                submissions_df['over_18'].append(None)
            try:
                submissions_df['created_utc'].append(submission.created_utc)
            except:
                submissions_df['created_utc'].append(None)
            try:
                created_at = datetime.fromtimestamp(submission.created_utc)
                submissions_df['created_at'].append(created_at)
            except:
                submissions_df['created_at'].append(None)

        submissions_df = pd.DataFrame.from_dict(data=submissions_df)
        return submissions_df

    # column_names exclude id column and created_at column
    def _create_df_from_reddit_models(self, reddit_models: list, column_names: list, reddit_property_names: list,
                                      id_col_name: str):
        assert len(column_names) == len(reddit_property_names)
        reddit_models_df = {
            col: [] for col in column_names
        }
        for rm in reddit_models:
            try:
                id_col = self.get_reddit_model_id(reddit_model=rm)
                reddit_models_df[id_col_name].append(id_col)
            except Exception as e:
                print('Cannot retrieve ID from model:', rm, id_col_name)
                print(e)
                reddit_models_df[id_col_name].append(None)
            for i in range(len(column_names)):
                cn = column_names[i]
                rpn = reddit_property_names[i]
                try:
                    reddit_models_df[cn].append(rm.__getattribute__(rpn))
                except:
                    reddit_models_df[cn].append(None)
            try:
                created_at = datetime.fromtimestamp(rm.created_utc)
                reddit_models_df['created_at'].append(created_at)
            except:
                reddit_models_df['created_at'].append(None)

        return reddit_models_df

    def _create_authors_df(self, authors):
        authors_df = {
            'redditor_id': [],
            'username': [],
            'link_karma': [],
            'comment_karma': [],
            'icon_img': [],
            'has_verified_email': [],
            'is_employee': [],
            'is_mod': [],
            'is_gold': [],
            'is_suspended': [],
            'created_at': [],
            'created_utc': [],
        }
        for author in authors:
            try:
                author_id = self.get_reddit_model_id(reddit_model=author)
                authors_df['redditor_id'].append(author_id)
            except:
                print('THIS SHOULD NOT HAPPEN 2')
                authors_df['redditor_id'].append(None)
            try:
                authors_df['username'].append(author.name)
            except:
                authors_df['username'].append(None)
            try:
                authors_df['link_karma'].append(author.link_karma)
            except:
                authors_df['link_karma'].append(None)
            try:
                authors_df['comment_karma'].append(author.comment_karma)
            except:
                authors_df['comment_karma'].append(None)
            try:
                authors_df['icon_img'].append(author.icon_img)
            except:
                authors_df['icon_img'].append(None)
            try:
                authors_df['has_verified_email'].append(author.has_verified_email)
            except:
                authors_df['has_verified_email'].append(None)
            try:
                authors_df['is_employee'].append(author.is_employee)
            except:
                authors_df['is_employee'].append(None)
            try:
                authors_df['is_mod'].append(author.is_mod)
            except:
                authors_df['is_mod'].append(None)
            try:
                authors_df['is_gold'].append(author.is_gold)
            except:
                authors_df['is_gold'].append(None)
            try:
                authors_df['is_suspended'].append(author.is_suspended)
            except:
                authors_df['is_suspended'].append(None)
            try:
                authors_df['created_utc'].append(author.created_utc)
            except:
                authors_df['created_utc'].append(None)
            try:
                created_at = datetime.fromtimestamp(author.created_utc)
                authors_df['created_at'].append(created_at)
            except:
                authors_df['created_at'].append(None)

        authors_df = pd.DataFrame.from_dict(data=authors_df)
        return authors_df

    def _create_comments_df(self, comments: list, submission_ids: list, subreddit_id: str):
        comments_df = {
            'comment_id': [],
            'author': [],
            'submission': submission_ids,
            'subreddit': [subreddit_id] * len(submission_ids),
            'body': [],
            'score': [],
            'distinguished': [],
            'is_submitter': [],
            'parent_id': [],
            'permalink': [],
            'created_at': [],
            'created_utc': [],
        }
        for comment in comments:
            try:
                comment_id = self.get_reddit_model_id(reddit_model=comment)
                comments_df['comment_id'].append(comment_id)
            except:
                print('THIS SHOULD NOT HAPPEN 3')
                comments_df['comment_id'].append(None)
            try:
                author_id = self.get_reddit_model_id(comment.author)
                comments_df['author'].append(author_id)
            except:
                print('THIS SHOULD NOT HAPPEN 4')
                comments_df['author'].append(None)
            try:
                comments_df['body'].append(comment.body)
            except:
                comments_df['body'].append(None)
            try:
                comments_df['score'].append(comment.score)
            except:
                comments_df['score'].append(None)
            try:
                comments_df['distinguished'].append(comment.distinguished)
            except:
                comments_df['distinguished'].append(None)
            try:
                comments_df['is_submitter'].append(comment.is_submitter)
            except:
                comments_df['is_submitter'].append(None)
            try:
                comments_df['parent_id'].append(comment.parent_id)
            except:
                comments_df['parent_id'].append(None)
            try:
                comments_df['permalink'].append(comment.permalink)
            except:
                comments_df['permalink'].append(None)
            try:
                comments_df['created_utc'].append(comment.created_utc)
            except:
                comments_df['created_utc'].append(None)
            try:
                created_at = datetime.fromtimestamp(comment.created_utc)
                comments_df['created_at'].append(created_at)
            except:
                comments_df['created_at'].append(None)

        comments_df = pd.DataFrame.from_dict(data=comments_df)
        return comments_df

    def perform_query(self, query: str, keyword: str, keyword_parts: list, subreddit_instance: praw.models.Subreddit):
        # Returns list of praw.models.Submission instances
        fetched_submissions = self.search_subreddit(query=query, subreddit_instance=subreddit_instance)

        # Before registering submissions, we need to register authors
        authors = []  # List of author instances
        keywords = [keyword] * len(fetched_submissions)
        has_exact_keyword = []
        for fs in fetched_submissions:
            try:
                author_instance = fs.author
                authors.append(author_instance)
            except Exception as e:
                print('Cannot get author from submission', fs.id)
                print(e)
                authors.append(None)
            try:
                for kp in keyword_parts:
                    if kp.lower() not in fs.title.lower():
                        has_exact_keyword.append(False)
                        break
                else:
                    has_exact_keyword.append(True)
            except:
                has_exact_keyword.append(None)

        assert len(authors) == len(fetched_submissions)

        # authors_df = self._create_authors_df(authors)
        column_names = ['username', 'link_karma', 'comment_karma', 'icon_img', 'has_verified_email', 'is_employee',
                        'is_mod', 'is_gold', 'is_suspended', 'created_utc']
        reddit_property_names = column_names.copy()
        reddit_property_names[0] = 'name'
        authors_df = self._create_df_from_reddit_models(reddit_models=authors,
                                                        column_names=column_names,
                                                        reddit_property_names=reddit_property_names,
                                                        id_col_name='redditor_id')
        try:
            subreddit_id = subreddit_instance.fullname
        except:
            subreddit_id = subreddit_instance.name

        submissions_df = self._create_submissions_df(submissions=fetched_submissions,
                                                     authors=authors_df['redditor_id'].tolist(),
                                                     keywords=keywords,
                                                     has_exact_keyword=has_exact_keyword,
                                                     subreddit_id=subreddit_id)
        assert authors_df.shape[0] == submissions_df.shape[0]

        all_comments = []
        comment_submissions = []
        for fs in fetched_submissions:
            try:
                fs.comments.replace_more(limit=None)
                for c in fs.comments.list():
                    all_comments.append(c)
                    comment_submissions.append(self.get_reddit_model_id(fs))
            except Exception as e:
                print('ERROR: Cannot replace more comments')
                print(fs.fullname)
                continue

        assert len(all_comments) == len(comment_submissions)
        comments_df = self._create_comments_df(comments=all_comments, submission_ids=comment_submissions,
                                               subreddit_id=subreddit_id)

        return authors_df, submissions_df, comments_df

    def register_reddit_model(self, df: pd.DataFrame, table_name: str, id_col: str):
        # Register one-by-one in case there are any duplicates (same author appears in multiple results)
        for i in range(df.shape[0]):
            row_df = df.iloc[i: i + 1, :]
            try:
                self.generic_db.insert_into_table(table_name=table_name, data=row_df, id_col=id_col)
            except:
                print(f'ERROR: Cannot register {table_name}')
                print(row_df)

    def search_for_keywords(self, subreddit_instance: praw.models.Subreddit):
        assert isinstance(subreddit_instance, praw.models.Subreddit)

        for keyword in self.search_keywords:
            keys = keyword.replace('AND ', '').split()
            query = ' AND '.join(f'title:{key}' for key in keys)
            keyword_parts = keyword.split(' AND ')
            print('Searching for keyword:', keyword)
            authors_df, submissions_df, comments_df = self.perform_query(query=query,
                                                                         keyword=keyword,
                                                                         keyword_parts=keyword_parts,
                                                                         subreddit_instance=subreddit_instance)
            self.register_reddit_model(df=authors_df, table_name='redditors', id_col='redditor_id')
            self.register_reddit_model(df=submissions_df, table_name='submissions', id_col='submission_id')
            self.register_reddit_model(df=comments_df, table_name='comments', id_col='comment_id')

    def search_reddit(self):
        for i, r in self.all_subs.iterrows():
            subreddit_name = r['display_name']
            print('Now searching:', subreddit_name)
            subreddit_instance = self.reddit.subreddit(subreddit_name)
            self.search_for_keywords(subreddit_instance=subreddit_instance)


rl = RedditLookup()
rl.search_reddit()
