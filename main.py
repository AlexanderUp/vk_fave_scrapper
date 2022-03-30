# encoding:utf-8
# main script for vk_scrapper.py

import os
import logging

import db_models

from pprint import pprint
from sqlalchemy.orm import mapper

from vk_scrapper import VK_Scrapper

from utils import parse_url_file
from utils import read_source_dir
from utils import parse_url
from utils import get_hash


SOURCE_FOLDER = 'source'


if __name__ == '__main__':
    print('*'*125)

    logging.basicConfig(level=logging.INFO)
    logging.info('Logging started.')

    vk = VK_Scrapper()
    print(vk)
    print(vk._session)
    print('-'*25)

    path_to_source_dir = os.path.join(os.getcwd(), SOURCE_FOLDER)
    print(f'>>>> Source: {path_to_source_dir}')
    print('-'*25)
    url_count = 0
    for source in read_source_dir(path_to_source_dir):
        urls = parse_url_file(source)
        url_count += len(urls)
        vk.update_db(urls)
    vk.close()

    print(f'URLs processed: {url_count}')
    print('-'*25)
