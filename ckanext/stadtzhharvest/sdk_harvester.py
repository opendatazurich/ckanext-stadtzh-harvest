import json
import logging

import requests
from ckan.lib.munge import munge_title_to_name
from requests.exceptions import HTTPError, JSONDecodeError

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.stadtzhharvest.utils import (
    stadtzhharvest_create_new_context,
    stadtzhharvest_create_package,
)

log = logging.getLogger(__name__)


class StadtzhSDKHarvester(HarvesterBase):
    """Harvester for Statistik Stadt Zürich from SDK (Städtischer DatenKatalog)."""

    def info(self):
        return {
            "name": "stadtzh_sdk_harvester",
            "title": "SDK harvester for the City of Zürich",
            "description": "Harvester from from SDK (Städtischer DatenKatalog) for the"
            " City of Zürich",
        }

    def validate_config(self, config_str):
        # config_obj = json.loads(config_str)

        return config_str

    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
        else:
            self.config = {}

        log.debug(f"Using config: {self.config}")

    def gather_stage(self, harvest_job):
        log.debug("In StadtzhSDKHarvester gather_stage")
        self._set_config(harvest_job.source.config)
        json_export_url = harvest_job.source.url.rstrip("/")

        r = requests.get(json_export_url)
        try:
            r.raise_for_status()
        except HTTPError as e:
            self._save_gather_error(
                f"Got error from source url {json_export_url}: {e}",
                harvest_job,
            )
            return []

        try:
            datasets = r.json()
        except JSONDecodeError as e:
            self._save_gather_error(
                f"Couldn't decode JSON from source url {json_export_url}: {e}"
            )
            return []

        ids = []
        gathered_dataset_names = []
        for dataset in datasets:
            dataset_name = munge_title_to_name(dataset["title"]).strip("-")
            log.debug(f"Gathering dataset {dataset_name}")
            gathered_dataset_names.append(dataset_name)
            dataset["name"] = dataset_name

            obj = HarvestObject(
                guid=dataset_name, job=harvest_job, content=json.dumps(dataset)
            )
            obj.save()
            log.debug(f"Added dataset {dataset_name} to the queue")
            ids.append(obj.id)

            # todo: check for deleted datasets

        return ids

    def fetch_stage(self, harvest_object):
        log.debug("In StadtzhSDKHarvester fetch_stage")
        # Nothing to do here
        return True

    def import_stage(self, harvest_object):
        log.debug("In StadtzhSDKHarvester import_stage")
        self._set_config(harvest_object.job.source.config)

        if not harvest_object:
            log.error("No harvest object received")
            self._save_object_error("No harvest object received", harvest_object)
            return False

        package_dict = json.loads(harvest_object.content)
        context = stadtzhharvest_create_new_context()
        log.warning(package_dict)

        # Simple fields
        # Groups
        # Tags
        # Attributes
        # Actual data

        dataset_id = stadtzhharvest_create_package(package_dict, harvest_object)
        log.warning(dataset_id)

        return True
