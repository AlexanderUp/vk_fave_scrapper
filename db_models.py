# ecoding:utf-8
# database models for vk_scrapper.py


from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean


metadata = MetaData()


url_table = Table('urls', metadata,
                Column('id', Integer, primary_key=True),
                Column('url', String, unique=True, nullable=False),
                Column('album_name', String, nullable=False),
                Column('owner_id', String, nullable=False),
                Column('photo_id', String, nullable=False),
                Column('is_downloaded', Boolean),
                Column('is_deleted', Boolean),
                Column('resource_url', String),
                Column('hash', String),
                )


class URL():

    def __init__(self, url, album_name, owner_id, photo_id):
        self.url = url
        self.album_name = album_name
        self.owner_id = owner_id
        self.photo_id = photo_id
        self.is_downloaded = False
        self.is_deleted = False
        self.resource_url = None
        self.hash = None

    def __repr__(self):
        return f'<URL({self.url})>'
