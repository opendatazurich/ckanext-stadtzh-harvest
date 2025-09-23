import json
import os

import pytest
from ckan.tests import helpers

import ckanext.stadtzhharvest.harvester as plugin
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
