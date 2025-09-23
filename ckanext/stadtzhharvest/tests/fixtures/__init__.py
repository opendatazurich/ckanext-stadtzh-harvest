import pytest
from ckan.tests.helpers import config

from ckanext.harvest import queue
from ckanext.stadtzhtheme.plugin import (
    create_dataType,
    create_updateInterval,
)


@pytest.fixture
def clean_db(reset_db, migrate_db_for):
    reset_db()
    migrate_db_for("harvest")

    # Cleaning the db gets rid of our custom tag vocabularies, so create them again
    create_dataType()
    create_updateInterval()


@pytest.fixture
def clean_queues():
    queue.purge_queues()
