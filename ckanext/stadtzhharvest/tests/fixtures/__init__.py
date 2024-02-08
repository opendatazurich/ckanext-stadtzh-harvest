# Copied from ckanext-harvester.

import pytest
from alembic.util import CommandError
from ckan.tests.helpers import config

import ckanext.harvest.model as harvest_model
from ckanext.harvest import queue
from ckanext.stadtzhtheme.plugin import StadtzhThemePlugin


@pytest.fixture
def clean_db(reset_db, migrate_db_for):
    reset_db()

    try:
        migrate_db_for("harvest")
    except CommandError:
        # ckanext-harvest has switched to using Alembic migrations, but this change
        # is not yet released: https://github.com/ckan/ckanext-harvest/pull/540
        pass

    # Cleaning the db gets rid of our custom tag vocabularies, so create them again
    plugin = StadtzhThemePlugin()
    plugin.configure(config=config)


@pytest.fixture
def harvest_setup():
    try:
        harvest_model.setup()
    except AttributeError:
        # harvest_model.setup() has been removed in the new version of ckanext-harvest
        # https://github.com/ckan/ckanext-harvest/pull/540
        pass


@pytest.fixture
def clean_queues():
    queue.purge_queues()
