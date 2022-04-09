# encoding:utf-8
# auxiliary utilities for vk_scrapper.py

# https://api.vk.com/method/users.get?user_id=210700286&v=5.131

import os
import re
import logging
import requests
import json


from bs4 import BeautifulSoup
from fnmatch import fnmatch
from hashlib import sha256
from collections import namedtuple


ACCESS_TOKEN = os.environ.get('ACCESS_TOKEN')

SOURCE_FILE_ENCODING = 'windows-1251'
FILENAME_PATTERN = 'likes*.html'
MAX_SIZE = 1024 * 1024 # one megabyte
MAX_CHUNK_SIZE = 1024 * 1024 # one megabyte

BASE_URL = 'https://api.vk.com/method/'
API_VERSION = '5.131'
GET_METHOD = 'photos.get'
GET_BY_ID_METHOD = 'photos.getById'
GET_METHOD_URL_TEMPLATE = 'https://api.vk.com/method/{base_method}' \
                '?access_token={access_token}' \
                '&album_id={album_name}' \
                '&owner_id={owner_id}' \
                '&photo_ids={photo_id}' \
                '&v={api_version}'
GET_BY_ID_METHOD_URL_TEMPLATE = 'https://api.vk.com/method/{base_method}' \
                '?access_token={access_token}' \
                '&photos={photos_id}' \
                '&v={api_version}'
PATH_TO_SAVE = os.environ.get('PATH_TO_SAVE')
HEADERS = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit 537.36 (KHTML, like Gecko) Chrome', 'Accept':'text/html,application/xhtml+xml, applicateion/xml;q=0.9,image/webp, */*;q=0.8'}

URL_PATTERN = re.compile('https://vk.com/(?P<album_name>(wall)|(photo))(?P<owner_id>[-]?\d+)_(?P<photo_id>\d+).*')
FILE_NAME_PATTERN = re.compile('https://sun[0-9]+-[0-9]+.userapi.com/imp[\w]/[\w/-]*/(?P<file_name>[\w-]*.jpg)\?.*')

Parsed_URL = namedtuple('parsed_url', 'album_name owner_id photo_id')


def read_source_dir(source_dir, filename_pattern=FILENAME_PATTERN):
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if fnmatch(file, filename_pattern):
                path = os.path.join(root, file)
                yield path
    return None

def parse_url_file(source):
    logging.info(f'Processing: {source}')
    with open(source, 'r', encoding=SOURCE_FILE_ENCODING) as f:
        bs = BeautifulSoup(f.read(), features='lxml')
    urls_items = bs.find_all(attrs={'class':'item__main'})
    urls = [url.get_text() for url in urls_items]
    return urls

def parse_url(url, url_pattern=URL_PATTERN):
    match_object = re.match(url_pattern, url)
    parsed_url = Parsed_URL(match_object.group('album_name'),
                            match_object.group('owner_id'),
                            match_object.group('photo_id'))
    return parsed_url

def get_json(queried_url_object,
            base_method=GET_BY_ID_METHOD,
            access_token=ACCESS_TOKEN,
            api_version=API_VERSION,
            headers=HEADERS):
    '''Get appropriate json response for url request by specified method.'''
    if base_method == GET_METHOD:
        url = GET_METHOD_URL_TEMPLATE.format(base_method=base_method,
                                    access_token=access_token,
                                    album_name=queried_url_object.album_name,
                                    owner_id=queried_url_object.owner_id,
                                    photo_id=queried_url_object.photo_id,
                                    api_version=api_version)
    if base_method == GET_BY_ID_METHOD:
        photos_id = '_'.join((queried_url_object.owner_id, queried_url_object.photo_id))
        logging.debug(f'Processing photo [{photos_id}]')
        url = GET_BY_ID_METHOD_URL_TEMPLATE.format(base_method=base_method,
                                    access_token=access_token,
                                    photos_id=photos_id,
                                    api_version=api_version)
    logging.debug(f'Getting json for [{url}]')
    r = requests.get(url, headers)
    return r.status_code, r.content

def parse_json_file(json_file):
    if not fnmatch(json_file, '*.json'):
        logging.error(f'Unsupport file format: {json_file}')
        raise TypeError('Unsupport file format.')
    with open(json_file, 'rb') as f:
        logging.info(f'Processing: {json_file}')
        response = json.load(f)
    if 'error' in response:
        error_code = response['error']['error_code']
        error_msg = response['error']['error_msg']
        raise KeyError(error_code, error_msg)
    if 'orig_photo' in response['response']:
        return response['response'][0]['orig_photo']['url']
    if 'response' in response:
        return response['response'][0]['sizes'][-1]['url']

def parse_json_response(response):
    if 'error' in response:
        error_code = response['error']['error_code']
        error_msg = response['error']['error_msg']
        raise KeyError(error_code, error_msg)
    if 'orig_photo' in response['response']:
        return response['response'][0]['orig_photo']['url']
    if 'response' in response:
        return response['response'][0]['sizes'][-1]['url']

def get_file_path_to_save(url, path_to_save=PATH_TO_SAVE, file_name_pattern=FILE_NAME_PATTERN):
    match_obj = re.match(file_name_pattern, url)
    file_name = match_obj.group('file_name')
    file_path = os.path.join(path_to_save, file_name)
    return file_path

def download_file(url, file_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        logging.info(f'Downloaded <{url}> to {file_path}')
    else:
        logging.info(f'Error during GET-request: {response.status_code}')
    return None

def get_hash(file, chunk_size=MAX_CHUNK_SIZE):
    hasher = sha256()
    with open(file, 'rb') as f:
        chunk = f.read(chunk_size)
        while chunk:
            hasher.update(chunk)
            chunk = f.read(chunk_size)
    return hasher.hexdigest()
