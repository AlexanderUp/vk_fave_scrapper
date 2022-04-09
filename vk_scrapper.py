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
import re
import time
import json

from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker
from io import BytesIO

import db_models as dbm

from db_models import URL
from utils import parse_url
from utils import get_json
from utils import parse_json_response
from utils import get_file_path_to_save
from utils import download_file
from utils import get_hash


mapper(dbm.URL, dbm.url_table)


PATH_TO_DB = 'url_db.sqlite3'
FOLDER_JSON = 'json_files'
QUERY_YIELD_PER_COUNT = 20
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

    def _query_urls(self):
        yield from self._session.query(URL).yield_per(QUERY_YIELD_PER_COUNT)

    def query_url_by_id(self, url_id):
        return self._session.query(URL).filter(URL.id==url_id).first()

    def _save_json(self, queried_url_object, json_string, folder=FOLDER_JSON):
        splited_url = re.split('https://vk.com/', queried_url_object.url)
        path = os.path.join(os.getcwd(), folder, f'{queried_url_object.id}_{splited_url[1]}.json')
        logging.debug(f'Path to save: {path}')
        with open(path, 'wb') as f:
            f.write(json_string)
        return None

    def _process_given_url(self, queried_url_object):
        status_code, response_content = get_json(queried_url_object)
        if status_code == 200:
            self._save_json(queried_url_object, response_content)
            json_file_obj = BytesIO()
            json_file_obj.write(response_content)
            json_file_obj.seek(0)
            json_response = json.load(json_file_obj)
            try:
                url = parse_json_response(json_response)
            except KeyError as err:
                logging.error(f'Error occured: {err} with id({queried_url_object.id})[{queried_url_object}]')
                queried_url_object.resource_url = 'Error'
            else:
                queried_url_object.resource_url = url
                logging.info(f'Processed... id({queried_url_object.id})[{queried_url_object}]')
            finally:
                self._session.add(queried_url_object)
                json_file_obj.close()
        else:
            logging.error(f'Status code: {status_code}')
        return None

    def process_urls(self):
        for queried_url_object in self._query_urls():
            self._process_given_url(queried_url_object)
            time.sleep(TIME_OUT)
        try:
            self._session.commit()
        except sqlalchemy.exc.SQLAlchemyError as err:
            logging.error(f'Error occured: {err}')
            logging.error(f'Error-prone object: {queried_url_object}')
            self._session.rollback()
            logging.error('Session has been rolled back.')
        return None

    def download_images(self):
        count = self._session.query(URL).filter(URL.resource_url.is_not('Error')).count()
        logging.info(f'Downloadable URL count: {count}')
        for url_object in self._session.query(URL).filter(URL.resource_url.is_not('Error')).yield_per(QUERY_YIELD_PER_COUNT):
            url = url_object.resource_url
            try:
                file_path = get_file_path_to_save(url)
            except AttributeError as err:
                logging.error(f'Error occured: {err}')
                logging.error(f'Error-prone URL: {url}')
                continue
            else:
                try:
                    download_file(url, file_path)
                except Exception as err:
                    logging.error(f'Error occured: {err}')
                else:
                    file_hash = get_hash(file_path)
                    url_object.hash = file_hash
                    url_object.is_downloaded = True
                    self._session.add(url_object)
            time.sleep(TIME_OUT)
        try:
            self._session.commit()
        except qlalchemy.exc.SQLAlchemyError as err:
            logging.error(f'Error occured: {err}')
            logging.error(f'Error-prone object: {url_object}')
            self._session.rollback()
            logging.error('Session has been rolled back.')
        return None

    def normalize_db(self):
        empty_url_objects_count = self._session.query(URL).filter(URL.resource_url.is_(None)).count()
        logging.info(f'Empty URL objects count: {empty_url_objects_count}')
        duplicated_resource_urls = self._session.query(URL).filter(URL.resource_url.is_not('Error')).group_by(URL.resource_url).order_by(URL.id.asc())
        for duplicated_url_object in duplicated_resource_urls.having(func.count(URL.resource_url)>1):
            curr_obj_dup_count = self._session.query(URL).filter(URL.resource_url == duplicated_url_object.resource_url).count()
            logging.info(f'Count: {curr_obj_dup_count}, {duplicated_url_object}')
            sub_query = self._session.query(URL).filter(URL.resource_url == duplicated_url_object.resource_url).all()
            for url_object in sub_query[1:]:
                self._session.delete(url_object)
                logging.info(f'Deleted: {url_object}')
        try:
            self._session.commit()
        except sqlalchemy.exc.SQLAlchemyError as err:
            logging.error(f'Error occured: {err}')
            self._session.rollback()
            logging.error('Session has been rolled back.')
        return None
