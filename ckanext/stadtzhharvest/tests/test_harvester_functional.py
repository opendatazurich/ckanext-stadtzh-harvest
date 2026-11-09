import json
import os
import shutil

import pytest
from ckan.lib.helpers import url_for
from ckan.tests import helpers

from ckanext.harvest.tests import factories as harvest_factories
from ckanext.harvest.tests.lib import run_harvest
from ckanext.stadtzhharvest.harvester import StadtzhHarvester

HARVESTER_URL = "http://stadthzh"
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class FunctionalHarvestTest(object):
    def create_harvest_source(self, config=None):
        source_dict = {
            "title": "Stadt ZH Source",
            "name": "test-stadtzh-source",
            "url": HARVESTER_URL,
            "source_type": "stadtzh_harvester",
        }
        if config is not None:
            source_dict["config"] = json.dumps(config)

        return harvest_factories.HarvestSource(**source_dict)

    def update_harvest_source(self, config=None):
        source_dict = {
            "title": "Stadt ZH Source",
            "name": "test-stadtzh-source",
            "url": HARVESTER_URL,
            "source_type": "stadtzh_harvester",
        }
        if config is not None:
            source_dict["config"] = json.dumps(config)

        harvest_source = helpers.call_action("harvest_source_update", {}, **source_dict)

        return harvest_source

    def _test_harvest_create(self, num_objects, config=None):
        """Create and run a harvest source with the given config. Confirm that the
        expected number of datasets are created. Return the harvester results and the
        harvest source id (in case needed for further tests).
        """
        harvest_source = self.create_harvest_source(config=config)

        run_harvest(HARVESTER_URL, StadtzhHarvester())

        # Check that correct amount of datasets were created
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)
        assert results["count"] == num_objects

        return results, harvest_source["id"]

    def _test_harvest_update(
        self, num_objects, mock_dropzone, dropzone_path, meta_xml_path, config=None
    ):
        # First run, will create datasets
        harvest_source = self.create_harvest_source(config=config)

        run_harvest(HARVESTER_URL, StadtzhHarvester())

        # Check that correct amount of datasets were created
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)
        assert results["count"] == num_objects

        # Change metadata in source
        with open(meta_xml_path, "r") as meta_file:
            meta = meta_file.read()
        meta = meta.replace("</titel>", " (updated)</titel>")
        with open(meta_xml_path, "w") as meta_file:
            meta_file.write(meta)

        # add new file to dropzone
        dataset_path = os.path.join(
            __location__, "fixtures", mock_dropzone, "test_dataset"
        )
        shutil.copytree(dataset_path, os.path.join(dropzone_path, "test_dataset"))

        # Run a second job
        run_harvest(HARVESTER_URL, StadtzhHarvester())

        # Check that one more dataset has been harvested
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)

        assert results["count"] == num_objects + 1
        return results

    def _test_harvest_update_resource(self, num_objects, meta_xml_path, config=None):
        # First run, will create datasets
        harvest_source = self.create_harvest_source(config=config)

        run_harvest(HARVESTER_URL, StadtzhHarvester())

        # Change resource metadata in source
        with open(meta_xml_path, "r") as meta_file:
            meta = meta_file.read()
        meta = meta.replace("</beschreibung>", " (updated)</beschreibung>")
        with open(meta_xml_path, "w") as meta_file:
            meta_file.write(meta)

        # Run a second job
        run_harvest(HARVESTER_URL, StadtzhHarvester())

        # Check that we still have the same number of datasets
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)

        assert results["count"] == num_objects
        return results

    def _test_harvest_delete_resource(self, num_objects, dropzone_path, config=None):
        # First run, will create datasets
        harvest_source = self.create_harvest_source(config=config)

        run_harvest(HARVESTER_URL, StadtzhHarvester())

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)

        assert results["count"] == num_objects

        # Remove resources from datasets
        for root, dirs, files in os.walk(dropzone_path):
            for file in files:
                if file not in ["meta.xml"]:
                    os.remove(os.path.join(root, file))

        # Run a second job
        run_harvest(HARVESTER_URL, StadtzhHarvester())

        # Check that we still have the same number of datasets
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)

        assert results["count"] == num_objects
        return results

    def _test_last_job_status(
        self,
        harvester_id,
        status="Finished",
        added_count=0,
        updated_count=0,
        deleted_count=0,
        not_modified_count=0,
        errored_count=0,
        error_messages=None,
    ):
        if error_messages is None:
            error_messages = []

        # Get the harvest source with the updated status
        harvest_source = helpers.call_action("harvest_source_show", id=harvester_id)
        last_job_status = harvest_source["status"]["last_job"]
        assert last_job_status["status"] == status

        assert len(last_job_status["object_error_summary"]) == errored_count
        assert last_job_status["stats"]["added"] == added_count
        assert last_job_status["stats"]["updated"] == updated_count
        assert last_job_status["stats"]["deleted"] == deleted_count
        assert last_job_status["stats"]["not modified"] == not_modified_count
        assert last_job_status["stats"]["errored"] == errored_count

        obj_summary = last_job_status["object_error_summary"]
        for msg in error_messages:
            assert msg in obj_summary[0]["message"], (
                "Error message does not match: %r" % obj_summary
            )


@pytest.mark.ckan_config("ckan.plugins", "stadtzhtheme harvest stadtzh_harvester")
@pytest.mark.usefixtures(
    "with_plugins",
    "clean_db",
    "clean_index",
    "clean_queues",
)
class TestStadtzhHarvestFunctional(FunctionalHarvestTest):
    def test_harvest_create_test_dropzone(self):
        data_path = os.path.join(__location__, "fixtures", "test_dropzone")
        test_config = {
            "data_path": data_path,
            "metafile_dir": "",
            "update_datasets": True,
            "update_date_last_modified": False,
        }

        results, _ = self._test_harvest_create(1, config=test_config)
        results = results["results"]
        assert len(results) == 1
        assert results[0]["name"] == "test_dataset"
        assert results[0]["title"] == "Administrative Einteilungen Stadt Zürich"
        assert results[0]["license_id"] == "cc-by"
        assert results[0]["updateInterval"][0] == "woechentlich"
        assert results[0]["dataType"][0] == "Einzeldaten"
        assert len(results[0]["resources"]) == 1

    def test_harvest_create_dwh(self):
        data_path = os.path.join(__location__, "fixtures", "DWH")
        test_config = {
            "data_path": data_path,
            "metafile_dir": "",
            "update_datasets": True,
            "update_date_last_modified": False,
        }

        results, _ = self._test_harvest_create(3, config=test_config)
        assert len(results["results"]) == 3
        for result in results["results"]:
            expected_titles = [
                "Geburten nach Jahr, Geschlecht und Stadtquartier",
                "Test Nachnamen in der Stadt Zürich",
                "Daten der permanenten Velozählstellen - Stundenwerte",
            ]

            assert result["title"] in expected_titles, (
                "Title does not match result: %s" % result
            )

    def test_harvest_create_geo(self):
        data_path = os.path.join(__location__, "fixtures", "GEO")
        test_config = {
            "data_path": data_path,
            "metafile_dir": "DEFAULT",
            "update_datasets": False,
            "update_date_last_modified": True,
        }

        results, _ = self._test_harvest_create(2, config=test_config)
        assert len(results["results"]) == 2
        for result in results["results"]:
            expected_titles = ["Alterswohnung", "Amtshaus"]
            assert result["title"] in expected_titles, (
                "Title does not match result: %s" % result
            )

    def test_geo_with_resources(self):
        data_path = os.path.join(__location__, "fixtures", "test_geo_dropzone")
        test_config = {
            "data_path": data_path,
            "metafile_dir": "DEFAULT",
            "update_datasets": False,
            "update_date_last_modified": True,
        }

        results, _ = self._test_harvest_create(1, config=test_config)
        assert len(results["results"]) == 1
        result = results["results"][0]

        assert result["title"] == "Administrative Einteilungen Stadt Zürich"
        assert result["license_id"] == "cc-zero"
        assert len(result["resources"]) == 4

        test_json = next(r for r in result["resources"] if r["name"] == "test.json")
        assert test_json["description"] == "This is a test description"

        test_csv = next(r for r in result["resources"] if r["name"] == "test.csv")
        assert test_csv["description"] == ""

        wms = next(r for r in result["resources"] if r["name"] == "Web Map Service")
        assert wms["description"] == ""

        wfs = next(r for r in result["resources"] if r["name"] == "Web Feature Service")
        assert wfs["description"] == "Dies ist eine Spezial-Beschreibung"

    def test_harvest_create_with_dataset_prefix(self):
        data_path = os.path.join(__location__, "fixtures", "test_dropzone")
        test_config = {
            "data_path": data_path,
            "metafile_dir": "",
            "update_datasets": True,
            "update_date_last_modified": False,
            "dataset_prefix": "testprefix-",
        }

        results, _ = self._test_harvest_create(1, config=test_config)
        results = results["results"]
        assert len(results) == 1
        assert results[0]["name"] == "testprefix-test_dataset"
        assert results[0]["title"] == "Administrative Einteilungen Stadt Zürich"
        assert results[0]["license_id"] == "cc-by"
        assert results[0]["updateInterval"][0] == "woechentlich"
        assert results[0]["dataType"][0] == "Einzeldaten"
        assert len(results[0]["resources"]) == 1

    def test_fail_with_invalid_url_resources(self, app):
        data_path = os.path.join(__location__, "fixtures", "fail_dropzone")
        test_config = {
            "data_path": data_path,
            "metafile_dir": "DEFAULT",
            "update_datasets": False,
            "update_date_last_modified": True,
        }

        # harvesting this dropzone should not fail
        # but generate an error on the log
        results, harvester_id = self._test_harvest_create(
            num_objects=1, config=test_config
        )

        self._test_last_job_status(
            harvester_id=harvester_id,
            errored_count=1,
            added_count=1,
            error_messages=[
                "Error while handling action",
                "Invalid URL (CDATA)",
                "Please provide a valid URL",
            ],
        )

        # make sure other resources are there
        result = results["results"][0]
        assert len(result["resources"]) == 2
        try:
            assert next(r for r in result["resources"] if r["name"] == "fail.json")
            assert next(
                r for r in result["resources"] if r["name"] == "Web Map Service"
            )
        except StopIteration:
            raise AssertionError("Resources fail.json/Web Map Service not found")

        # make sure search still works after failed harvesting
        url = url_for("home.index")
        app.get(url, status=200)

    def test_delete_dataset(self):
        data_path = os.path.join(__location__, "fixtures", "DWH")
        test_config = {
            "data_path": data_path,
            "delete_missing_datasets": True,
            "metafile_dir": "",
            "update_datasets": True,
            "update_date_last_modified": True,
        }

        # harvesting the default DWH-dropzone
        results, harvester_id = self._test_harvest_create(
            num_objects=3, config=test_config
        )

        self._test_last_job_status(
            harvester_id=harvester_id,
            added_count=3,
        )

        # run a second harvest-job with updated dropzone-path
        # where two datasets are deleted
        data_path_deleted = os.path.join(
            __location__, "fixtures", "delete_dataset_dropzone"
        )
        test_config_deleted = {
            "data_path": data_path_deleted,
            "delete_missing_datasets": True,
            "metafile_dir": "",
            "update_datasets": True,
            "update_date_last_modified": True,
        }

        harvest_source = self.update_harvest_source(config=test_config_deleted)

        run_harvest(HARVESTER_URL, StadtzhHarvester())

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)
        assert results["count"] == 1

        self._test_last_job_status(
            harvester_id=harvester_id,
            updated_count=1,
            deleted_count=2,
        )

    def test_delete_dataset_when_source_has_more_than_ten_datasets(self):
        data_path = os.path.join(__location__, "fixtures", "GEO2")
        test_config = {
            "data_path": data_path,
            "metafile_dir": "DEFAULT",
            "update_datasets": False,
            "update_date_last_modified": True,
        }

        # harvesting the default DWH-dropzone
        results, harvester_id = self._test_harvest_create(
            num_objects=11, config=test_config
        )
        self._test_last_job_status(
            harvester_id=harvester_id,
            added_count=11,
        )

        # run a second harvest-job with updated dropzone-path
        # where two datasets are deleted
        data_path_deleted = os.path.join(
            __location__, "fixtures", "delete_dataset_dropzone_GEO2"
        )
        test_config_deleted = {
            "data_path": data_path_deleted,
            "delete_missing_datasets": True,
            "metafile_dir": "DEFAULT",
            "update_datasets": False,
            "update_date_last_modified": True,
        }

        harvest_source = self.update_harvest_source(config=test_config_deleted)

        run_harvest(HARVESTER_URL, StadtzhHarvester())

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq, rows=11)
        assert results["count"] == 3

        self._test_last_job_status(
            harvester_id=harvester_id,
            updated_count=3,
            deleted_count=8,
        )

    def test_harvest_update_dwh(self, temp_dir):
        data_path = os.path.join(__location__, "fixtures", "DWH")
        temp_data_path = os.path.join(temp_dir, "DWH")
        shutil.copytree(data_path, temp_data_path)

        test_config = {
            "data_path": temp_data_path,
            "metafile_dir": "",
            "update_datasets": True,
            "update_date_last_modified": False,
        }

        meta_xml_path = os.path.join(temp_dir, "DWH", "nachnamen_2014", "meta.xml")

        results = self._test_harvest_update(
            3,
            "test_dropzone",
            temp_data_path,
            meta_xml_path,
            config=test_config,
        )
        assert len(results["results"]) == 4

        # the title of 'nachnamen_2014' should be updated +
        # one dataset should be added
        for result in results["results"]:
            expected_titles = [
                "Geburten nach Jahr, Geschlecht und Stadtquartier",
                "Test Nachnamen in der Stadt Zürich (updated)",
                "Daten der permanenten Velozählstellen - Stundenwerte",
                "Administrative Einteilungen Stadt Zürich",
            ]

            assert result["title"] in expected_titles, (
                "Title does not match result: %s" % result
            )

    def test_harvest_update_geo(self, temp_dir):
        data_path = os.path.join(__location__, "fixtures", "GEO")
        temp_data_path = os.path.join(temp_dir, "GEO")
        shutil.copytree(data_path, temp_data_path)

        test_config = {
            "data_path": temp_data_path,
            "metafile_dir": "DEFAULT",
            "update_datasets": False,
            "update_date_last_modified": True,
        }

        meta_xml_path = os.path.join(temp_dir, "GEO", "amtshaus", "DEFAULT", "meta.xml")

        results = self._test_harvest_update(
            2,
            "test_geo_dropzone",
            temp_data_path,
            meta_xml_path,
            config=test_config,
        )
        assert len(results["results"]) == 3
        # since 'update_datasets' is set to False, no datasets should be
        # changed, but a new one should be there
        for result in results["results"]:
            expected_titles = [
                "Alterswohnung",
                "Amtshaus",
                "Administrative Einteilungen Stadt Zürich",
            ]
            assert result["title"] in expected_titles, (
                "Title does not match result: %s" % result
            )

    def test_harvest_update_resources_geo(self, temp_dir):
        data_path = os.path.join(__location__, "fixtures", "test_geo_dropzone")
        temp_data_path = os.path.join(temp_dir, "GEO")
        shutil.copytree(data_path, temp_data_path)

        test_config = {
            "data_path": temp_data_path,
            "metafile_dir": "DEFAULT",
            "update_datasets": True,
            "update_date_last_modified": True,
        }

        meta_xml_path = os.path.join(
            temp_dir, "GEO", "test_dataset", "DEFAULT", "meta.xml"
        )

        results = self._test_harvest_update_resource(
            1, meta_xml_path, config=test_config
        )
        assert len(results["results"]) == 1
        # since 'update_datasets' is set to True, resources should be changed
        result = results["results"][0]
        test_json = next(r for r in result["resources"] if r["name"] == "test.json")
        assert test_json["description"] == "This is a test description (updated)"

    @pytest.mark.parametrize(
        "dropzone, num_datasets, delete_missing_resources, expect_deleted",
        [
            ("DWH", 3, None, True),
            ("DWH", 3, True, True),
            ("DWH", 3, False, False),
            ("GEO", 2, None, True),
            ("GEO", 2, True, True),
            ("GEO", 2, False, False),
        ],
    )
    def test_delete_missing_resources(
        self, temp_dir, dropzone, num_datasets, delete_missing_resources, expect_deleted
    ):
        data_path = os.path.join(__location__, "fixtures", dropzone)
        temp_data_path = os.path.join(temp_dir, dropzone)
        shutil.copytree(data_path, temp_data_path)

        test_config = {
            "data_path": temp_data_path,
            "update_datasets": True,
            "update_date_last_modified": False,
        }
        if dropzone == "GEO":
            test_config["metafile_dir"] = "DEFAULT"
        if delete_missing_resources is not None:
            test_config["delete_missing_resources"] = delete_missing_resources

        results = self._test_harvest_delete_resource(
            num_objects=num_datasets,
            dropzone_path=temp_data_path,
            config=test_config,
        )

        for result in results["results"]:
            if expect_deleted:
                assert result["resources"] == []
            else:
                assert len(result["resources"]) > 0
