# -*- coding: utf-8 -*-

import nose
import os
import json
import tempfile
import shutil
import ckantoolkit.tests.helpers as h

import ckanext.harvest.model as harvest_model
from ckanext.harvest import queue

import ckanext.stadtzhharvest.harvester as plugin
import ckanext.stadtzhtheme.plugin as theme

eq_ = nose.tools.eq_
assert_true = nose.tools.assert_true
assert_raises = nose.tools.assert_raises

__location__ = os.path.realpath(
    os.path.join(
        os.getcwd(),
        os.path.dirname(__file__)
    )
)


class TestStadtzhHarvester(h.FunctionalTestBase):

    def test_load_metadata_from_path(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = 'test_dataset'
        data_path = os.path.join(
            __location__,
            'fixtures',
            'test_dropzone'
        )

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
        eq_(metadata['license_id'], u'cc-zero')
        eq_(len(metadata['resource_metadata']), 0)

    def test_load_metadata_from_path_empty(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = 'empty_dataset'
        data_path = os.path.join(
            __location__,
            'fixtures',
            'test_dropzone'
        )

        test_meta_xml_path = os.path.join(
            data_path,
            dataset_folder,
            'meta.xml'
        )

        with assert_raises(plugin.MetaXmlNotFoundError):
            harvester._load_metadata_from_path(
                test_meta_xml_path,
                dataset_folder,
                dataset_folder
            )

    def test_load_metadata_license(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = 'cc-by-dataset'
        data_path = os.path.join(
            __location__,
            'fixtures',
            'license_dropzone'
        )

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
        eq_(metadata['license_id'], u'cc-by')

    def test_load_metadata_resource_descriptions(self):
        harvester = plugin.StadtzhHarvester()
        dataset_folder = 'test_dataset'
        data_path = os.path.join(
            __location__,
            'fixtures',
            'test_geo_dropzone'
        )

        test_meta_xml_path = os.path.join(
            data_path,
            dataset_folder,
            'DEFAULT',
            'meta.xml'
        )

        metadata = harvester._load_metadata_from_path(
            test_meta_xml_path,
            dataset_folder,
            dataset_folder
        )
        eq_(metadata['datasetFolder'], dataset_folder)
        eq_(metadata['datasetID'], dataset_folder)
        eq_(len(metadata['resource_metadata']), 1)

        assert 'test.csv' not in metadata['resource_metadata']
        assert 'test.json' in metadata['resource_metadata'], "test.json is not defined"
        eq_(metadata['resource_metadata']['test.json']['description'], u'This is a test description')


class FunctionalHarvestTest(object):
    @classmethod
    def setup_class(cls):
        h.reset_db()

        cls.gather_consumer = queue.get_gather_consumer()
        cls.fetch_consumer = queue.get_fetch_consumer()

    def setup(self):
        harvest_model.setup()

        queue.purge_queues()

        # create required tag vocabularies
        theme.create_updateInterval()
        theme.create_dataType()

        # create temp dir for this test
        self.temp_dir = tempfile.mkdtemp()

    def teardown(self):
        h.reset_db()
        shutil.rmtree(self.temp_dir)

    def _create_harvest_source(self, **kwargs):

        source_dict = {
            'title': 'Stadt ZH Source',
            'name': 'test-stadtzh-source',
            'url': 'http://stadthzh',
            'source_type': 'stadtzh_harvester',
        }

        source_dict.update(**kwargs)

        harvest_source = h.call_action('harvest_source_create',
                                       {}, **source_dict)

        return harvest_source

    def _create_harvest_job(self, harvest_source_id):

        harvest_job = h.call_action('harvest_job_create',
                                    {}, source_id=harvest_source_id)

        return harvest_job

    def _run_jobs(self, harvest_source_id=None):
        try:
            h.call_action('harvest_jobs_run',
                          {}, source_id=harvest_source_id)
        except Exception, e:
            if (str(e) == 'There are no new harvesting jobs'):
                pass

    def _gather_queue(self, num_jobs=1):

        for job in xrange(num_jobs):
            # Pop one item off the queue (the job id) and run the callback
            reply = self.gather_consumer.basic_get(
                queue='ckan.harvest.gather.test')

            # Make sure something was sent to the gather queue
            assert reply[2], 'Empty gather queue'

            # Send the item to the gather callback, which will call the
            # harvester gather_stage
            queue.gather_callback(self.gather_consumer, *reply)

    def _fetch_queue(self, num_objects=1):

        for _object in xrange(num_objects):
            # Pop item from the fetch queues (object ids) and run the callback,
            # one for each object created
            reply = self.fetch_consumer.basic_get(
                queue='ckan.harvest.fetch.test')

            # Make sure something was sent to the fetch queue
            assert reply[2], 'Empty fetch queue, the gather stage failed'

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


class TestStadtzhHarvestFunctional(FunctionalHarvestTest):

    def test_harvest_create_test_dropzone(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'test_dropzone'
        )
        test_config = json.dumps({
            'data_path': data_path,
            'metafile_dir': '',
            'metadata_dir': 'test-metadata',
            'update_datasets': True,
            'update_date_last_modified': False
        })

        results = self._test_harvest_create(1, config=test_config)['results']
        eq_(len(results), 1)
        eq_(results[0]['title'], u'Administrative Einteilungen Stadt Zürich')
        eq_(results[0]['license_id'], u'cc-by')
        eq_(len(results[0]['resources']), 1)

    def test_harvest_create_dwh(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'DWH'
        )
        test_config = json.dumps({
            'data_path': data_path,
            'metafile_dir': '',
            'metadata_dir': 'dwh-metadata',
            'update_datasets': True,
            'update_date_last_modified': False
        })

        results = self._test_harvest_create(3, config=test_config)
        eq_(len(results['results']), 3)
        for result in results['results']:
            expected_titles = [
                u'Geburten nach Jahr, Geschlecht und Stadtquartier',
                u'Test Nachnamen in der Stadt Zürich',
                u'Daten der permanenten Velozählstellen - Stundenwerte',
            ]

            assert result['title'] in expected_titles, "Title does not match result: %s" % result

    def test_harvest_create_geo(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'GEO'
        )
        test_config = json.dumps({
            'data_path': data_path,
            'metafile_dir': 'DEFAULT',
            'metadata_dir': 'geo-metadata',
            'update_datasets': False,
            'update_date_last_modified': True
        })

        results = self._test_harvest_create(2, config=test_config)
        eq_(len(results['results']), 2)
        for result in results['results']:
            expected_titles = ['Alterswohnung', 'Amtshaus']
            assert result['title'] in expected_titles, "Title does not match result: %s" % result

    def test_geo_with_resources(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'test_geo_dropzone'
        )
        test_config = json.dumps({
            'data_path': data_path,
            'metafile_dir': 'DEFAULT',
            'metadata_dir': 'geo-metadata',
            'update_datasets': False,
            'update_date_last_modified': True
        })

        results = self._test_harvest_create(1, config=test_config)
        eq_(len(results['results']), 1)
        result = results[0]

        eq_(result['title'], u'Administrative Einteilungen Stadt Zürich')
        eq_(result['license_id'], u'cc-zero')
        eq_(len(result['resources']), 4)

        test_json = next(r for r in result['resources'] if r["name"] == "test.json") 
        eq_(test_json['description'], u'This is a test description')

        test_csv = next(r for r in result['resources'] if r["name"] == "test.csv") 
        eq_(test_json['description'], u'')
        wms = next(r for r in result['resources'] if r["name"] == "Web Map Service") 
        eq_(wms['description'], u'')
        wfs = next(r for r in result['resources'] if r["name"] == "Web Feature Service") 
        eq_(wfs['description'], u'Dies ist eine Spezial-Beschreibung')

    def _test_harvest_create(self, num_objects, **kwargs):
        harvest_source = self._create_harvest_source(**kwargs)

        self._run_full_job(harvest_source['id'], num_objects=num_objects)

        # Check that two datasets were created
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source['id'])
        results = h.call_action('package_search', {}, fq=fq)
        eq_(results['count'], num_objects)
        return results

    def test_harvest_update_dwh(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'DWH'
        )
        temp_data_path = os.path.join(self.temp_dir, 'DWH')
        shutil.copytree(data_path, temp_data_path)

        test_config = json.dumps({
            'data_path': temp_data_path,
            'metafile_dir': '',
            'metadata_dir': 'dwh-metadata',
            'update_datasets': True,
            'update_date_last_modified': False
        })
        meta_xml_path = os.path.join(
            self.temp_dir,
            'DWH',
            'nachnamen_2014',
            'meta.xml'
        )

        results = self._test_harvest_update(3, 'test_dropzone', temp_data_path, meta_xml_path, config=test_config)
        eq_(len(results['results']), 4)

        # the title of 'nachnamen_2014' should be updated + 1 one dataset should be added
        for result in results['results']:
            expected_titles = [
                u'Geburten nach Jahr, Geschlecht und Stadtquartier',
                u'Test Nachnamen in der Stadt Zürich (updated)',
                u'Daten der permanenten Velozählstellen - Stundenwerte',
                u'Administrative Einteilungen Stadt Zürich',
            ]

            assert result['title'] in expected_titles, "Title does not match result: %s" % result

    def test_harvest_update_geo(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'GEO'
        )
        temp_data_path = os.path.join(self.temp_dir, 'GEO')
        shutil.copytree(data_path, temp_data_path)

        test_config = json.dumps({
            'data_path': temp_data_path,
            'metafile_dir': 'DEFAULT',
            'metadata_dir': 'geo-metadata',
            'update_datasets': False,
            'update_date_last_modified': True
        })
        meta_xml_path = os.path.join(
            self.temp_dir,
            'GEO',
            'amtshaus',
            'DEFAULT',
            'meta.xml'
        )

        results = self._test_harvest_update(2, 'test_geo_dropzone', temp_data_path, meta_xml_path, config=test_config)
        eq_(len(results['results']), 3)
        # since 'update_datasets' is set to False, no datasets should be changed
        # but a new one should be there
        for result in results['results']:
            expected_titles = [
                u'Alterswohnung',
                u'Amtshaus',
                u'Administrative Einteilungen Stadt Zürich',
            ]
            assert result['title'] in expected_titles, "Title does not match result: %s" % result

    def _test_harvest_update(self, num_objects, mock_dropzone, dropzone_path, meta_xml_path, **kwargs):
        harvest_source = self._create_harvest_source(**kwargs)

        # First run, will create datasets as previously tested
        self._run_full_job(harvest_source['id'], num_objects=num_objects)

        # Run the jobs to mark the previous one as Finished
        self._run_jobs()

        # change data in source
        with open(meta_xml_path, 'r') as meta_file:
            meta = meta_file.read()
        meta = meta.replace('</titel>', ' (updated)</titel>')
        with open(meta_xml_path, 'w') as meta_file:
            meta_file.write(meta)

        # add new file to dropzone
        dataset_path = os.path.join(
            __location__,
            'fixtures',
            mock_dropzone,
            'test_dataset'
        )
        shutil.copytree(
            dataset_path,
            os.path.join(dropzone_path, 'test_dataset')
        )

        # Run a second job
        self._run_full_job(harvest_source['id'], num_objects=num_objects+1)

        # Check that we still have two datasets
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source['id'])
        results = h.call_action('package_search', {}, fq=fq)

        eq_(results['count'], num_objects+1)
        return results
