# encoding:utf-8
# main script for vk_scrapper.py

import os
import logging
import json
import re

from pprint import pprint
from sqlalchemy.orm import mapper
from io import BytesIO

from vk_scrapper import VK_Scrapper
from utils import parse_url_file
from utils import read_source_dir
from utils import parse_url
from utils import get_json


SOURCE_FOLDER = 'source'
JSON_FOLDER = 'json_files'
DOWNLOAD_FOLDER = os.path.expanduser('~/Desktop/Python - my projects/vk_fave_img_scrapper/test_folder')


if __name__ == '__main__':
    print('*'*125)

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    logging.info('Logging started.')

    vk = VK_Scrapper()
    print(vk)
    print('-'*25)

    '''<<Defining source files directory>>'''
    # path_to_source_dir = os.path.join(os.getcwd(), SOURCE_FOLDER)
    # logging.info(f'>>>> Source dir: {path_to_source_dir}')
    # print('-'*25)

    '''<<Process file with likes in specified folder>>'''
    # url_count = 0
    # for source in read_source_dir(path_to_source_dir):
    #     urls = parse_url_file(source)
    #     url_count += len(urls)
    #     vk.update_db(urls)
    # logging.info(f'URLs processed: {url_count}')
    # print('-'*25)

    '''<<Check quantity of objects queried from db>>'''
    # total_yielded = 0
    # for u in vk._query_urls():
    #     total_yielded += 1
    # print(f'Total: {total_yielded}')
    # print('-'*25)

    '''<<Process urls from db>>'''
    # vk.process_urls()
    # print('-'*25)

    '''<<Normalize db - delete entries with empty resource_url and resource_url-duplicated entries>>'''
    # vk.normalize_db()
    # print('-'*25)

    '''<Downloading file with given url with VK_Scrupper>'''
    vk.download_images()
    print('-'*25)

    '''<<Close db in VK object>>'''
    vk.close()
    logging.info('VK DB closed.')
    print('-'*25)
