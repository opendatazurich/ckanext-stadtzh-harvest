# Copied from ckanext-harvester.

import pytest
from alembic.util import CommandError

from ckanext.harvest import queue


@pytest.fixture
def clean_db(reset_db, migrate_db_for):
    reset_db()
    try:
        migrate_db_for("harvest")
    except CommandError:
        # ckanext-harvest has switched to using Alembic migrations, but this change
        # is not yet released: https://github.com/ckan/ckanext-harvest/pull/540
        pass


@pytest.fixture
def clean_queues():
    queue.purge_queues()
