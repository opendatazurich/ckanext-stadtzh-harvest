# -*- coding: utf-8 -*-

import nose
import os
import json
from ckan.tests import helpers, factories
from ckan.lib.helpers import url_for

import ckanext.stadtzhharvest.harvester as plugin

eq_ = nose.tools.eq_
assert_true = nose.tools.assert_true

__location__ = os.path.realpath(
    os.path.join(
        os.getcwd(),
        os.path.dirname(__file__)
    )
)


class TestStadtzhHarvester(helpers.FunctionalTestBase):

    def test_load_metadata_from_path(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = 'test_dataset'
        data_path =  os.path.join(
            __location__,
            'fixtures',
            'dropzone'
        )
        test_config = {
            'data_path': data_path,
            'metafile_dir': ''
        }
        harvester._set_config(json.dumps(test_config))


        test_meta_xml_path = os.path.join(
            data_path,
            dataset_folder,
            'meta.xml'
        )
        
        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path,
            dataset_folder,
            dataset_folder
        )
        eq_(metadata['datasetFolder'], dataset_folder)
        eq_(metadata['datasetID'], dataset_folder)
        eq_(metadata['title'], u'Administrative Einteilungen Stadt Zürich')
