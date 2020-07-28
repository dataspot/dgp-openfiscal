import os
import logging

from flask import Blueprint, current_app, abort
from flask_jsonpify import jsonify

from sqlalchemy import create_engine

from babbage import CubeManager
from babbage.api import configure_api

from taxonomies.babbage_models import babbage_models

class CustomCubeManager(CubeManager):

    def __init__(self):
        db_connection_string = os.environ['DATASETS_DATABASE_URL']
        engine = create_engine(db_connection_string)
        super().__init__(engine)

    def list_cubes(self):
        """ List all available models in the DB """
        for instance in babbage_models.query()['result']:
            yield instance['key']

    def has_cube(self, name):
        """ Check if a cube exists. """
        return babbage_models.query_one(name)['success']

    def get_cube_model(self, name):
        return babbage_models.query_one(name)['result']['value']

manager = CustomCubeManager()


def get_package(slug):
    if manager.has_model(slug):
        return jsonify(manager.get_package(slug))
    else:
        abort(404)


def extra_server_init(app):
    logging.info('extra_server_init starting')
    app.register_blueprint(configure_api(app, manager), url_prefix='/api/3')
    app.add_url_rule('/api/3/info/<slug>/package','get_package', get_package, methods=['GET'])
    logging.info('extra_server_init done')