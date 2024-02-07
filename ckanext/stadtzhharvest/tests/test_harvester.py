# -*- coding: utf-8 -*-

import json
import os
import shutil
import tempfile

import pytest
from ckan.lib.helpers import url_for
from ckan.tests import helpers

import ckanext.stadtzhharvest.harvester as plugin
import ckanext.stadtzhtheme.plugin as theme
from ckanext.harvest import queue

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


@pytest.mark.ckan_config("ckan.plugins", "harvest stadtzh_harvester")
@pytest.mark.usefixtures("with_plugins", "clean_db", "clean_index")
class TestStadtzhHarvester(object):
    def test_load_metadata_from_path(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = "test_dataset"
        data_path = os.path.join(__location__, "fixtures", "test_dropzone")

        test_meta_xml_path = os.path.join(data_path, dataset_folder, "meta.xml")

        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path, dataset_folder, dataset_folder
        )
        assert metadata["datasetFolder"] == dataset_folder
        assert metadata["datasetID"] == dataset_folder
        assert metadata["title"] == "Administrative Einteilungen Stadt Zürich"
        assert metadata["license_id"] == "cc-by"
        assert len(metadata["resource_metadata"]) == 0

    def test_load_metadata_from_path_empty(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = "empty_dataset"
        data_path = os.path.join(__location__, "fixtures", "test_dropzone")

        test_meta_xml_path = os.path.join(data_path, dataset_folder, "meta.xml")

        with pytest.raises(plugin.MetaXmlNotFoundError):
            harvester._load_metadata_from_path(
                test_meta_xml_path, dataset_folder, dataset_folder
            )

    def test_load_metadata_license(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = "cc-by-dataset"
        data_path = os.path.join(__location__, "fixtures", "license_dropzone")

        test_meta_xml_path = os.path.join(data_path, dataset_folder, "meta.xml")

        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path, dataset_folder, dataset_folder
        )
        assert metadata["datasetFolder"] == dataset_folder
        assert metadata["datasetID"] == dataset_folder
        assert metadata["license_id"] == "cc-by"

    def test_load_metadata_no_license(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = "no-license-dataset"
        data_path = os.path.join(__location__, "fixtures", "license_dropzone")

        test_meta_xml_path = os.path.join(data_path, dataset_folder, "meta.xml")

        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path, dataset_folder, dataset_folder
        )
        assert metadata["datasetFolder"] == dataset_folder
        assert metadata["datasetID"] == dataset_folder
        assert metadata["license_id"] == "cc-zero"

    def test_load_metadata_resource_descriptions(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = "test_dataset"
        data_path = os.path.join(__location__, "fixtures", "test_geo_dropzone")

        test_meta_xml_path = os.path.join(
            data_path, dataset_folder, "DEFAULT", "meta.xml"
        )

        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path, dataset_folder, dataset_folder
        )
        assert metadata["datasetFolder"] == dataset_folder
        assert metadata["datasetID"] == dataset_folder
        assert len(metadata["resource_metadata"]) == 1

        assert "test.csv" not in metadata["resource_metadata"]
        assert "test.json" in metadata["resource_metadata"], "test.json is not defined"
        assert (
            metadata["resource_metadata"]["test.json"]["description"]
            == "This is a test description"
        )

    def test_load_metadata_attributes(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = "velozaehlstellen_stundenwerte"
        data_path = os.path.join(__location__, "fixtures", "DWH")

        test_meta_xml_path = os.path.join(data_path, dataset_folder, "meta.xml")

        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path, dataset_folder, dataset_folder
        )
        assert metadata["datasetFolder"] == dataset_folder
        assert metadata["datasetID"] == dataset_folder

        attributes = json.loads(metadata["sszFields"])
        assert len(attributes) == 9

        jahr = attributes[1]
        assert jahr[0] == "Jahr (technisch: Vkjahr Id)"
        assert jahr[1] == "Jahreszahl (z.B. 2012)"

        # attribute without tech. name
        anzahl = attributes[8]
        assert anzahl[0] == "Gezählte Velofahrten"
        assert anzahl[1] == "Anzahl Velos pro Stunde an der jeweiligen Messstelle"

    def test_load_metadata_groups_and_tags(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = "nachnamen_2014"
        data_path = os.path.join(__location__, "fixtures", "DWH")

        test_meta_xml_path = os.path.join(data_path, dataset_folder, "meta.xml")

        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path, dataset_folder, dataset_folder
        )
        assert metadata["datasetFolder"] == dataset_folder
        assert metadata["datasetID"] == dataset_folder

        # tags
        assert len(metadata["tags"]) == 5
        assert metadata["tags"] == [
            {"name": "sachdaten"},
            {"name": "tabellen"},
            {"name": "tag-mit-spaces"},
            {"name": "gross-klein"},
            {"name": "umlaute-auo"},
        ]

        # groups
        def check_group(id, name, title):
            group_result = helpers.call_action("group_show", {}, id=id)
            assert group_result["id"] == id
            assert group_result["name"] == name
            assert group_result["title"] == title

        assert len(metadata["groups"]) == 3
        check_group(metadata["groups"][0]["name"], "tourismus", "Tourismus")
        check_group(metadata["groups"][1]["name"], "freizeit", "Freizeit")
        check_group(metadata["groups"][2]["name"], "bevolkerung", "Bevölkerung")


class FunctionalHarvestTest(object):
    @classmethod
    def setup_class(cls):
        cls.gather_consumer = queue.get_gather_consumer()
        cls.fetch_consumer = queue.get_fetch_consumer()

    def setup(self):
        # create required tag vocabularies
        theme.create_updateInterval()
        theme.create_dataType()

        # create temp dir for this test
        self.temp_dir = tempfile.mkdtemp()

    def teardown(self):
        shutil.rmtree(self.temp_dir)

    def _create_harvest_source(self, **kwargs):
        source_dict = {
            "title": "Stadt ZH Source",
            "name": "test-stadtzh-source",
            "url": "http://stadthzh",
            "source_type": "stadtzh_harvester",
        }

        source_dict.update(**kwargs)

        harvest_source = helpers.call_action("harvest_source_create", {}, **source_dict)

        return harvest_source

    def _update_harvest_source(self, **kwargs):
        source_dict = {
            "title": "Stadt ZH Source",
            "name": "test-stadtzh-source",
            "url": "http://stadthzh",
            "source_type": "stadtzh_harvester",
        }

        source_dict.update(**kwargs)

        harvest_source = helpers.call_action("harvest_source_update", {}, **source_dict)

        return harvest_source

    def _create_harvest_job(self, harvest_source_id):
        harvest_job = helpers.call_action(
            "harvest_job_create", {}, source_id=harvest_source_id
        )

        return harvest_job

    def _run_jobs(self, harvest_source_id=None):
        try:
            helpers.call_action("harvest_jobs_run", {}, source_id=harvest_source_id)
        except Exception as e:
            if str(e) == "There are no new harvesting jobs":
                pass

    def _gather_queue(self, num_jobs=1):
        for job in range(num_jobs):
            # Pop one item off the queue (the job id) and run the callback
            reply = self.gather_consumer.basic_get(queue="ckan.harvest.gather.test")

            # Make sure something was sent to the gather queue
            assert reply[2], "Empty gather queue"

            # Send the item to the gather callback, which will call the
            # harvester gather_stage
            queue.gather_callback(self.gather_consumer, *reply)

    def _fetch_queue(self, num_objects=1):
        for _object in range(num_objects):
            # Pop item from the fetch queues (object ids) and run the
            # callback, one for each object created
            reply = self.fetch_consumer.basic_get(queue="ckan.harvest.fetch.test")

            # Make sure something was sent to the fetch queue
            assert reply[2], "Empty fetch queue, the gather stage failed"

            # Send the item to the fetch callback, which will call the
            # harvester fetch_stage and import_stage
            queue.fetch_callback(self.fetch_consumer, *reply)

    def _run_full_job(self, harvest_source_id, num_jobs=1, num_objects=1):
        # Create new job for the source
        self._create_harvest_job(harvest_source_id)

        # Run the job
        self._run_jobs(harvest_source_id)

        # Handle the gather queue
        self._gather_queue(num_jobs)

        # Handle the fetch queue
        self._fetch_queue(num_objects)


@pytest.mark.ckan_config("ckan.plugins", "stadtzhtheme harvest stadtzh_harvester")
@pytest.mark.usefixtures(
    "with_plugins",
    "clean_db",
    "clean_index",
    "clean_queues",
    "harvest_setup",
)
class TestStadtzhHarvestFunctional(FunctionalHarvestTest):
    def test_harvest_create_test_dropzone(self):
        data_path = os.path.join(__location__, "fixtures", "test_dropzone")
        test_config = json.dumps(
            {
                "data_path": data_path,
                "metafile_dir": "",
                "update_datasets": True,
                "update_date_last_modified": False,
            }
        )

        results = self._test_harvest_create(1, config=test_config)["results"]
        assert len(results) == 1
        assert results[0]["name"] == "test_dataset"
        assert results[0]["title"] == "Administrative Einteilungen Stadt Zürich"
        assert results[0]["license_id"] == "cc-by"
        assert results[0]["updateInterval"][0] == "woechentlich"
        assert results[0]["dataType"][0] == "Einzeldaten"
        assert len(results[0]["resources"]) == 1

    def test_harvest_create_dwh(self):
        data_path = os.path.join(__location__, "fixtures", "DWH")
        test_config = json.dumps(
            {
                "data_path": data_path,
                "metafile_dir": "",
                "update_datasets": True,
                "update_date_last_modified": False,
            }
        )

        results = self._test_harvest_create(3, config=test_config)
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
        test_config = json.dumps(
            {
                "data_path": data_path,
                "metafile_dir": "DEFAULT",
                "update_datasets": False,
                "update_date_last_modified": True,
            }
        )

        results = self._test_harvest_create(2, config=test_config)
        assert len(results["results"]) == 2
        for result in results["results"]:
            expected_titles = ["Alterswohnung", "Amtshaus"]
            assert result["title"] in expected_titles, (
                "Title does not match result: %s" % result
            )

    def test_geo_with_resources(self):
        data_path = os.path.join(__location__, "fixtures", "test_geo_dropzone")
        test_config = json.dumps(
            {
                "data_path": data_path,
                "metafile_dir": "DEFAULT",
                "update_datasets": False,
                "update_date_last_modified": True,
            }
        )

        results = self._test_harvest_create(1, config=test_config)
        assert len(results["results"]) == 1
        result = results["results"][0]

        print(result["resources"])

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
        test_config = json.dumps(
            {
                "data_path": data_path,
                "metafile_dir": "",
                "update_datasets": True,
                "update_date_last_modified": False,
                "dataset_prefix": "testprefix-",
            }
        )

        results = self._test_harvest_create(1, config=test_config)["results"]
        assert len(results) == 1
        assert results[0]["name"] == "testprefix-test_dataset"
        assert results[0]["title"] == "Administrative Einteilungen Stadt Zürich"
        assert results[0]["license_id"] == "cc-by"
        assert results[0]["updateInterval"][0] == "woechentlich"
        assert results[0]["dataType"][0] == "Einzeldaten"
        assert len(results[0]["resources"]) == 1

    def _test_harvest_create(self, num_objects, **kwargs):
        harvest_source = self._create_harvest_source(**kwargs)

        self._run_full_job(harvest_source["id"], num_objects=num_objects)

        # Check that correct amount of datasets were created
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)
        assert results["count"] == num_objects
        return results

    def test_fail_with_invalid_url_resources(self, app):
        data_path = os.path.join(__location__, "fixtures", "fail_dropzone")
        test_config = json.dumps(
            {
                "data_path": data_path,
                "metafile_dir": "DEFAULT",
                "update_datasets": False,
                "update_date_last_modified": True,
            }
        )

        # harvesting this dropzone should not fail
        # but generate an error on the log
        harvest_source = self._create_harvest_source(config=test_config)
        self._run_full_job(harvest_source["id"], num_objects=1)

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)
        assert results["count"] == 1

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        # Get the harvest source with the updated status
        harvest_source = helpers.call_action(
            "harvest_source_show", id=harvest_source["id"]
        )
        last_job_status = harvest_source["status"]["last_job"]
        assert last_job_status["status"] == "Finished"

        error_count = len(last_job_status["object_error_summary"])
        assert error_count == 1
        assert last_job_status["stats"]["added"] == 1
        assert last_job_status["stats"]["updated"] == 0
        assert last_job_status["stats"]["deleted"] == 0
        assert last_job_status["stats"]["not modified"] == 0
        assert last_job_status["stats"]["errored"] == 1

        obj_summary = last_job_status["object_error_summary"]
        assert "Error while handling action" in obj_summary[0]["message"], (
            "Error msg 1 does not match: %r" % obj_summary
        )
        assert "Invalid URL (CDATA)" in obj_summary[0]["message"], (
            "Error msg 2 does not match: %r" % obj_summary
        )
        assert "Please provide a valid URL" in obj_summary[0]["message"], (
            "Error msg 3 does not match: %r" % obj_summary
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
        test_config = json.dumps(
            {
                "data_path": data_path,
                "delete_missing_datasets": True,
                "metafile_dir": "",
                "update_datasets": True,
                "update_date_last_modified": True,
            }
        )

        # harvesting the default DWH-dropzone
        harvest_source = self._create_harvest_source(config=test_config)
        self._run_full_job(harvest_source["id"], num_objects=3)

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)
        assert results["count"] == 3

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        # Get the harvest source with the updated status
        harvest_source = helpers.call_action(
            "harvest_source_show", id=harvest_source["id"]
        )
        last_job_status = harvest_source["status"]["last_job"]
        assert last_job_status["status"] == "Finished"

        error_count = len(last_job_status["object_error_summary"])
        assert error_count == 0
        assert last_job_status["stats"]["added"] == 3
        assert last_job_status["stats"]["updated"] == 0
        assert last_job_status["stats"]["deleted"] == 0
        assert last_job_status["stats"]["not modified"] == 0
        assert last_job_status["stats"]["errored"] == 0

        # run a second harvest-job with updated dropzone-path
        # where two datasets are deleted
        data_path_deleted = os.path.join(
            __location__, "fixtures", "delete_dataset_dropzone"
        )
        test_config_deleted = json.dumps(
            {
                "data_path": data_path_deleted,
                "delete_missing_datasets": True,
                "metafile_dir": "",
                "update_datasets": True,
                "update_date_last_modified": True,
            }
        )

        harvest_source = self._update_harvest_source(config=test_config_deleted)
        self._run_full_job(harvest_source["id"], num_objects=3)

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)
        assert results["count"] == 1

        # Get the harvest source with the updated status
        harvest_source = helpers.call_action(
            "harvest_source_show", id=harvest_source["id"]
        )
        last_job_status = harvest_source["status"]["last_job"]
        assert last_job_status["status"] == "Finished"

        error_count = len(last_job_status["object_error_summary"])
        assert error_count == 0
        assert last_job_status["stats"]["added"] == 0
        assert last_job_status["stats"]["updated"] == 1
        assert last_job_status["stats"]["deleted"] == 2
        assert last_job_status["stats"]["not modified"] == 0
        assert last_job_status["stats"]["errored"] == 0

    def test_delete_dataset_when_source_has_more_than_ten_datasets(self):
        data_path = os.path.join(__location__, "fixtures", "GEO2")
        test_config = json.dumps(
            {
                "data_path": data_path,
                "metafile_dir": "DEFAULT",
                "update_datasets": False,
                "update_date_last_modified": True,
            }
        )

        # harvesting the default DWH-dropzone
        harvest_source = self._create_harvest_source(config=test_config)
        self._run_full_job(harvest_source["id"], num_objects=11)

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq, rows=11)
        assert results["count"] == 11

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        # Get the harvest source with the updated status
        harvest_source = helpers.call_action(
            "harvest_source_show", id=harvest_source["id"]
        )
        last_job_status = harvest_source["status"]["last_job"]
        assert last_job_status["status"] == "Finished"

        error_count = len(last_job_status["object_error_summary"])
        assert error_count == 0
        assert last_job_status["stats"]["added"] == 11
        assert last_job_status["stats"]["updated"] == 0
        assert last_job_status["stats"]["deleted"] == 0
        assert last_job_status["stats"]["not modified"] == 0
        assert last_job_status["stats"]["errored"] == 0

        # run a second harvest-job with updated dropzone-path
        # where two datasets are deleted
        data_path_deleted = os.path.join(
            __location__, "fixtures", "delete_dataset_dropzone_GEO2"
        )
        test_config_deleted = json.dumps(
            {
                "data_path": data_path_deleted,
                "delete_missing_datasets": True,
                "metafile_dir": "DEFAULT",
                "update_datasets": False,
                "update_date_last_modified": True,
            }
        )

        harvest_source = self._update_harvest_source(config=test_config_deleted)
        self._run_full_job(harvest_source["id"], num_objects=11)

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq, rows=11)
        assert results["count"] == 3

        # Get the harvest source with the updated status
        harvest_source = helpers.call_action(
            "harvest_source_show", id=harvest_source["id"]
        )
        last_job_status = harvest_source["status"]["last_job"]
        assert last_job_status["status"] == "Finished"

        error_count = len(last_job_status["object_error_summary"])
        assert error_count == 0
        assert last_job_status["stats"]["added"] == 0
        assert last_job_status["stats"]["updated"] == 3
        assert last_job_status["stats"]["deleted"] == 8
        assert last_job_status["stats"]["not modified"] == 0
        assert last_job_status["stats"]["errored"] == 0

    def test_harvest_update_dwh(self):
        data_path = os.path.join(__location__, "fixtures", "DWH")
        temp_data_path = os.path.join(self.temp_dir, "DWH")
        shutil.copytree(data_path, temp_data_path)

        test_config = json.dumps(
            {
                "data_path": temp_data_path,
                "metafile_dir": "",
                "update_datasets": True,
                "update_date_last_modified": False,
            }
        )
        meta_xml_path = os.path.join(self.temp_dir, "DWH", "nachnamen_2014", "meta.xml")

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

    def test_harvest_update_geo(self):
        data_path = os.path.join(__location__, "fixtures", "GEO")
        temp_data_path = os.path.join(self.temp_dir, "GEO")
        shutil.copytree(data_path, temp_data_path)

        test_config = json.dumps(
            {
                "data_path": temp_data_path,
                "metafile_dir": "DEFAULT",
                "update_datasets": False,
                "update_date_last_modified": True,
            }
        )
        meta_xml_path = os.path.join(
            self.temp_dir, "GEO", "amtshaus", "DEFAULT", "meta.xml"
        )

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

    def _test_harvest_update(
        self, num_objects, mock_dropzone, dropzone_path, meta_xml_path, **kwargs
    ):
        harvest_source = self._create_harvest_source(**kwargs)

        # First run, will create datasets as previously tested
        self._run_full_job(harvest_source["id"], num_objects=num_objects)

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        # change data in source
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
        self._run_full_job(harvest_source["id"], num_objects=num_objects + 1)

        # Check that we still have two datasets
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)

        assert results["count"] == num_objects + 1
        return results

    def test_harvest_update_resources_geo(self):
        data_path = os.path.join(__location__, "fixtures", "test_geo_dropzone")
        temp_data_path = os.path.join(self.temp_dir, "GEO")
        shutil.copytree(data_path, temp_data_path)

        test_config = json.dumps(
            {
                "data_path": temp_data_path,
                "metafile_dir": "DEFAULT",
                "update_datasets": True,
                "update_date_last_modified": True,
            }
        )
        meta_xml_path = os.path.join(
            self.temp_dir, "GEO", "test_dataset", "DEFAULT", "meta.xml"
        )

        results = self._test_harvest_update_resource(
            1, meta_xml_path, config=test_config
        )
        assert len(results["results"]) == 1
        # since 'update_datasets' is set to True, resources should be changed
        result = results["results"][0]
        test_json = next(r for r in result["resources"] if r["name"] == "test.json")
        assert test_json["description"] == "This is a test description (updated)"

    def _test_harvest_update_resource(self, num_objects, meta_xml_path, **kwargs):
        harvest_source = self._create_harvest_source(**kwargs)

        # First run, will create datasets as previously tested
        self._run_full_job(harvest_source["id"], num_objects=num_objects)

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        # change data in source
        with open(meta_xml_path, "r") as meta_file:
            meta = meta_file.read()
        meta = meta.replace("</beschreibung>", " (updated)</beschreibung>")
        with open(meta_xml_path, "w") as meta_file:
            meta_file.write(meta)

        # Run a second job
        self._run_full_job(harvest_source["id"], num_objects=num_objects)

        # Check that we still have two datasets
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source["id"])
        results = helpers.call_action("package_search", {}, fq=fq)

        assert results["count"] == num_objects
        return results
