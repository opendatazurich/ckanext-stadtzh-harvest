import json
import logging
import traceback

import requests
from ckan.lib.munge import munge_title_to_name
from requests.exceptions import HTTPError, JSONDecodeError

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.stadtzhharvest.utils import (
    stadtzhharvest_create_package,
    stadtzhharvest_find_or_create_organization,
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
            package_dict = self._map_metadata(dataset)

            obj = HarvestObject(
                guid=dataset_name, job=harvest_job, content=json.dumps(package_dict)
            )
            obj.save()
            log.debug(f"Added dataset {dataset_name} to the queue")
            ids.append(obj.id)
            gathered_dataset_names.append(dataset_name)

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

        try:
            # todo: Return 'unchanged' if the package has not changed
            return stadtzhharvest_create_package(package_dict, harvest_object)
        except Exception as e:
            log.exception(e)
            self._save_object_error(
                (
                    "Unable to get content for package: %s: %r / %s"
                    % (harvest_object.guid, e, traceback.format_exc())
                ),
                harvest_object,
            )
            return False

    def _map_metadata(self, dataset):
        """Map the exported dataset from SDK to a package_dict that we can give to CKAN
        to create/update a package.
        """
        log.warning(dataset)
        package_dict = {
            "title": dataset.get("title", ""),
            "url": dataset.get("url", ""),
            "notes": dataset.get("notes", ""),
            "author": ", ".join([dataset.get("department"),
                                 dataset.get("service_department")]),
            "maintainer": dataset.get("maintainer", "Open Data Zürich"),
            "maintainer_email": dataset.get("maintainer", "opendata@zuerich.ch"),
            "license_id": dataset.get("license", "cc-zero"),
            "tags": self._get_tags(dataset),
            "groups": self._get_groups(dataset),
            "spatialRelationship": dataset.get("spatialRelationship", ""),
            "dateFirstPublished": dataset.get("dateFirstPublished", ""),
            "dateLastUpdated": dataset.get("dateLastUpdated", ""),
            "updateInterval": dataset.get("updateInterval", ""),
            "legalInformation": dataset.get("legalInformation", []),
            "timeRange": dataset.get("timeRange", ""),
            "sszBemerkungen": dataset.get("sszBemerkungen", ""),
            "dataQuality": dataset.get("dataQuality", ""),
            "sszFields": self._get_attributes(dataset),
        }

        # todo: not in the JSON export: license_id
        # todo: for legalInformation we get a list, but this should be a string
        # todo: we need a link to the location of the actual data

        stadtzhharvest_find_or_create_organization(package_dict)

        return True
