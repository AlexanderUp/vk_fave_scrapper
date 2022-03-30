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
BASE_METOD = 'photos.get'

OWNER_ID = '-89704005'
ALBUM_ID = 'wall' # 'photo'
PHOTO_IDS = '457246452'

API_VERSION = '5.131'

URL_TEMPLATE = 'https://api.vk.com/method/{}?access_token={}&owner_id={}&album_id={}&photo_ids={}&v={}'


class VK_Scrapper():

    def __init__(self, path_to_db=None):
        self._path_to_db = path_to_db or os.path.join(os.getcwd(), PATH_TO_DB)
        self._session = self._create_session(self._path_to_db)

    def __repr__(self):
        return f'<VK DB({self._path_to_db})>'

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

    def close(self):
        self._session.close()
        return None

    def download_photos(self,
                        base_method=BASE_METOD,
                        access_token=ACCESS_TOKEN,
                        owner_id=OWNER_ID,
                        album_id=ALBUM_ID,
                        photo_ids=PHOTO_IDS,
                        api_version=API_VERSION):
        url = URL_TEMPLATE.format(base_method, access_token, owner_id, album_id, photo_ids, api_version)
        print(f'Getting... [{url}]')
        r = requests.get(url)
        return r.status_code, r.content
