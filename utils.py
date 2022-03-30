# encoding:utf-8
# auxiliary utilities for vk_scrapper.py

# https://api.vk.com/method/users.get?user_id=210700286&v=5.131

import os
import re
import logging


from bs4 import BeautifulSoup
from fnmatch import fnmatch
from hashlib import sha256
from collections import namedtuple


SOURCE_FILE_ENCODING = 'windows-1251'
FILENAME_PATTERN = 'likes*.html'
MAX_SIZE = 1024 * 1024 # one megabyte
MAX_CHUNK_SIZE = 1024 * 1024 # one megabyte

URL_PATTERN = re.compile('https://vk.com/(?P<album_name>(wall)|(photo))(?P<owner_id>[-]?\d+)_(?P<photo_id>\d+).*')

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

def get_hash(file, chunk_size=MAX_CHUNK_SIZE):
    hasher = sha256()
    with open(file, 'br') as f:
        chunk = f.read(chunk_size)
        while chunk:
            hasher.update(chunk)
            chunk = f.read(chunk_size)
    return hasher.hexdigest()
