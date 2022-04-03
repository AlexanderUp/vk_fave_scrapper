# encoding:utf-8
# downloading LIKED images from vk.com
#
# PRE-REQUIREMENTS:
# 1. Existance of list of liked images (can be obtained by downloading of private info,
# see https://vk.com/data_protection).
# 2. Application access token.


import os
import sqlalchemy
import logging
import requests
import json
import re
import time

import db_models as dbm

from sqlalchemy import create_engine
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker

from db_models import URL
from utils import parse_url


mapper(dbm.URL, dbm.url_table)


ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')
PATH_TO_DB = 'url_db.sqlite3'
BASE_URL = 'https://api.vk.com/method/'
BASE_METHOD = 'photos.get'
API_VERSION = '5.131'
URL_TEMPLATE = 'https://api.vk.com/method/{base_method}' \
                '?access_token={access_token}' \
                '&album_id={album_name}' \
                '&owner_id={owner_id}' \
                '&photo_ids={photo_id}' \
                '&v={api_version}'
FOLDER_JSON = 'json_files'
TIME_OUT = .5


class VK_Scrapper():

    def __init__(self, path_to_db=None):
        self._path_to_db = path_to_db or os.path.join(os.getcwd(), PATH_TO_DB)
        self._session = self._create_session(self._path_to_db)

    def __repr__(self):
        return f'<VK DB({self._path_to_db})>'

    # TODO: context manager (use of <close> method)
    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def _create_session(self, path_to_db):
        engine = create_engine('sqlite:///' + path_to_db)
        dbm.metadata.create_all(bind=engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

    def close(self):
        self._session.close()
        return None

    def update_db(self, urls):
        for url in urls:
            logging.debug(f'Processing: {url}')
            parsed_url = parse_url(url)
            album_name = parsed_url.album_name
            owner_id = parsed_url.owner_id
            photo_id = parsed_url.photo_id

            url_entry = URL(url, album_name, owner_id, photo_id)
            self._session.add(url_entry)
            try:
                self._session.commit()
            except sqlalchemy.exc.SQLAlchemyError as err:
                logging.error(f'Error occured: {err}')
                logging.error(f'Error-prone url: {url}')
                logging.error(f'Parsed url: {url_entry}')
                self._session.rollback()
                logging.error('Session has been rolled back.')
                continue
        return None

    def _query_url(self):
        # yield from self._session.query(URL).slice(0, 200).yield_per(20)
        yield from self._session.query(URL).yield_per(20)

    def _get_json(self,
                queried_url_object,
                base_method=BASE_METHOD,
                access_token=ACCESS_TOKEN,
                api_version=API_VERSION,
                ):
        '''Get appropriate json response for url request.'''
        url = URL_TEMPLATE.format(base_method=base_method,
                                    access_token=access_token,
                                    album_name=queried_url_object.album_name,
                                    owner_id=queried_url_object.owner_id,
                                    photo_id=queried_url_object.photo_id,
                                    api_version=api_version)
        logging.debug(f'Getting json for [{url}]')
        r = requests.get(url)
        return r.status_code, r.content

    def _save_json(self, queried_url_object, json_string, folder=FOLDER_JSON):
        splited_url = re.split('https://vk.com/', queried_url_object.url)
        path = os.path.join(os.getcwd(), folder, f'{queried_url_object.id}_{splited_url[1]}.json')
        logging.debug(f'Path to save: {path}')
        with open(path, 'w') as f:
            json.dump(json_string.decode('utf-8'), f)
        return None

    def _process_current_url(self, queried_url_object):
        status_code, response_content = self._get_json(queried_url_object)
        if status_code == 200:
            self._save_json(queried_url_object, response_content)
        else:
            logging.error(f'Error with {queried_url_object}')
            logging.error(f'status_code: {status_code}')
        return None

    def process_urls(self):
        for queried_url_object in self._query_url():
            logging.info(f'Processing url... id({queried_url_object.id})[{queried_url_object}]')
            self._process_current_url(queried_url_object)
            time.sleep(TIME_OUT)
        return None

    def _parse_json(self, json_string):
        pass
