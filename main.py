# encoding:utf-8
# main script for vk_scrapper.py

import os
import logging
import json

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

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    logging.info('Logging started.')

    vk = VK_Scrapper()
    print(vk)
    print('-'*25)

    path_to_source_dir = os.path.join(os.getcwd(), SOURCE_FOLDER)
    logging.info(f'>>>> Source dir: {path_to_source_dir}')
    print('-'*25)

    # url_count = 0
    # for source in read_source_dir(path_to_source_dir):
    #     urls = parse_url_file(source)
    #     url_count += len(urls)
    #     vk.update_db(urls)
    # vk.close()
    #
    # logging.info(f'URLs processed: {url_count}')
    # print('-'*25)

    # queried_url_object = vk._query_url()
    # print(queried_url_object)
    # print(queried_url_object.album_name)
    # print(queried_url_object.owner_id)
    # print(queried_url_object.photo_id)


    # total_yielded = 0
    # for u in vk._query_url():
    #     print(u)
    #     total_yielded += 1
    # # vk.process_url(queried_url_object)
    # print(f'Total: {total_yielded}')
    # print('-'*25)

    vk.process_urls()
    print('-'*25)

    json_str = '{"error":{"error_code":100,' \
    '"error_msg":"One of the parameters specified was missing or invalid: album_id is invalid",' \
    '"request_params":[{"key":"album_id","value":"photo"},' \
    '{"key":"owner_id","value":"-71375900"},' \
    '{"key":"photo_ids","value":"457242935"},' \
    '{"key":"v","value":"5.131"},' \
    '{"key":"method","value":"photos.get"},' \
    '{"key":"oauth","value":"1"}]}}' \

    # vk._save_json(queried_url_object.url, json_str)

    # p = os.path.expanduser('~/Desktop/Python - my projects/vk_fave_img_scrapper/json_files/photo-71375900_457242935?reply=8396.json')
    #
    # with open(p, 'r') as f:
    #     st = json.load(f)
    # print('-'*25)
    # print('json read:')
    # print(st)
    # print(st == json_str)
