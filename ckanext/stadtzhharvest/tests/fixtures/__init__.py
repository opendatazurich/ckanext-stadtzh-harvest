# Copied from ckanext-harvester.

import pytest

from ckanext.harvest import queue


@pytest.fixture
def clean_db(reset_db, migrate_db_for):
    reset_db()


@pytest.fixture
def clean_queues():
    queue.purge_queues()
