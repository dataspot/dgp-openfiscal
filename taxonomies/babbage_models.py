import os

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import Column, String

from etl_server.db_utils import Common, ModelsBase


# ## SQL DB
Base = declarative_base()


# ## USERS
class BabbageModel(Base, Common):
    __tablename__ = 'etl_babbage_models'


class BabbageModels(ModelsBase):

    def __init__(self, connection_string=None):
        super().__init__(Base, BabbageModel, connection_string)


babbage_models = BabbageModels(os.environ['ETLS_DATABASE_URL'])