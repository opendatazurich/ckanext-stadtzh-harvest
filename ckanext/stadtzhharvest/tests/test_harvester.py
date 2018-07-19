# -*- coding: utf-8 -*-

import nose
import os
import json
import ckantoolkit.tests.helpers as h

import ckanext.harvest.model as harvest_model
from ckanext.harvest import queue

import ckanext.stadtzhharvest.harvester as plugin

eq_ = nose.tools.eq_
assert_true = nose.tools.assert_true

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


class FunctionalHarvestTest(object):
    @classmethod
    def setup_class(cls):
        h.reset_db()

        cls.gather_consumer = queue.get_gather_consumer()
        cls.fetch_consumer = queue.get_fetch_consumer()

    def setup(self):
        harvest_model.setup()

        queue.purge_queues()

    def teardown(cls):
        h.reset_db()

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
        test_config = {
            'data_path': data_path,
            'metafile_dir': '',
            'metadata_dir': 'test-metadata',
            'update_datasets': True,
            'update_date_last_modified': False
        }

        result = self._test_harvest_create(1, config=json.dumps(test_config))[0]
        eq_(result['title'], u'Administrative Einteilungen Stadt Zürich')

    def test_harvest_create_dwh(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'DWH'
        )
        test_config = {
            'data_path': data_path,
            'metafile_dir': '',
            'metadata_dir': 'dwh-metadata',
            'update_datasets': True,
            'update_date_last_modified': False
        }

        results = self._test_harvest_create(3, config=test_config)
        for result in results:
            assert result['title'] in (u'Geburten nach Jahr, Geschlecht und Stadtquartier',
                                       u'Test Nachnamen in der Stadt Zürich'
                                       u'Daten der permanenten Velozählstellen - Stundenwerte')

    def test_harvest_create_geo(self):
        data_path = os.path.join(
            __location__,
            'fixtures',
            'GEO'
        )
        test_config = {
            'data_path': data_path,
            'metafile_dir': 'DEFAULT',
            'metadata_dir': 'geo-metadata',
            'update_datasets': False,
            'update_date_last_modified': True
        }

        results = self._test_harvest_create(2, config=test_config)
        for result in results:
            assert result['title'] in ('Alterswohnung', 'Amtshaus')

    def _test_harvest_create(self, num_objects, **kwargs):

        from pprint import pprint
        pprint(kwargs)
        harvest_source = self._create_harvest_source(**kwargs)

        self._run_full_job(harvest_source['id'], num_objects=num_objects)

        # Check that two datasets were created
        fq = "+type:dataset harvest_source_id:{0}".format(harvest_source['id'])
        results = h.call_action('package_search', {}, fq=fq)
        eq_(results['count'], num_objects)
	return results


    # def test_harvest_update_rdf(self):

    #     self._test_harvest_update(self.rdf_mock_url,
    #                               self.rdf_content,
    #                               self.rdf_content_type)

    # def _test_harvest_update(self, url, content, content_type):
    #     # Mock the GET request to get the file
    #     httpretty.register_uri(httpretty.GET, url,
    #                            body=content, content_type=content_type)

    #     # The harvester will try to do a HEAD request first so we need to mock
    #     # this as well
    #     httpretty.register_uri(httpretty.HEAD, url,
    #                            status=405, content_type=content_type)

    #     harvest_source = self._create_harvest_source(url)

    #     # First run, will create two datasets as previously tested
    #     self._run_full_job(harvest_source['id'], num_objects=2)

    #     # Run the jobs to mark the previous one as Finished
    #     self._run_jobs()

    #     # Mock an update in the remote file
    #     new_file = content.replace('Example dataset 1',
    #                                'Example dataset 1 (updated)')
    #     httpretty.register_uri(httpretty.GET, url,
    #                            body=new_file, content_type=content_type)

    #     # Run a second job
    #     self._run_full_job(harvest_source['id'], num_objects=2)

    #     # Check that we still have two datasets
    #     fq = "+type:dataset harvest_source_id:{0}".format(harvest_source['id'])
    #     results = h.call_action('package_search', {}, fq=fq)

    #     eq_(results['count'], 2)

    #     # Check that the dataset was updated
    #     for result in results['results']:
    #         assert result['title'] in ('Example dataset 1 (updated)',
    #                                    'Example dataset 2')

    # def test_harvest_update_existing_resources(self):

    #     existing, new = self._test_harvest_update_resources(self.rdf_mock_url,
    #                               self.rdf_content_with_distribution_uri,
    #                               self.rdf_content_type)
    #     eq_(new['uri'], 'https://data.some.org/catalog/datasets/1/resource/1')
    #     eq_(new['uri'], existing['uri'])
    #     eq_(new['id'], existing['id'])

    # def test_harvest_update_new_resources(self):

    #     existing, new = self._test_harvest_update_resources(self.rdf_mock_url,
    #                               self.rdf_content_with_distribution,
    #                               self.rdf_content_type)
    #     eq_(existing['uri'], '')
    #     eq_(new['uri'], '')
    #     nose.tools.assert_is_not(new['id'], existing['id'])

    # def _test_harvest_update_resources(self, url, content, content_type):
    #     # Mock the GET request to get the file
    #     httpretty.register_uri(httpretty.GET, url,
    #                            body=content, content_type=content_type)

    #     # The harvester will try to do a HEAD request first so we need to mock
    #     # this as well
    #     httpretty.register_uri(httpretty.HEAD, url,
    #                            status=405, content_type=content_type)

    #     harvest_source = self._create_harvest_source(url)

    #     # First run, create the dataset with the resource
    #     self._run_full_job(harvest_source['id'], num_objects=1)

    #     # Run the jobs to mark the previous one as Finished
    #     self._run_jobs()

    #     # get the created dataset
    #     fq = "+type:dataset harvest_source_id:{0}".format(harvest_source['id'])
    #     results = h.call_action('package_search', {}, fq=fq)
    #     eq_(results['count'], 1)

    #     existing_dataset = results['results'][0]
    #     existing_resource = existing_dataset.get('resources')[0]

    #     # Mock an update in the remote file
    #     new_file = content.replace('Example resource 1',
    #                                'Example resource 1 (updated)')
    #     httpretty.register_uri(httpretty.GET, url,
    #                            body=new_file, content_type=content_type)

    #     # Run a second job
    #     self._run_full_job(harvest_source['id'])

    #     # get the updated dataset
    #     new_results = h.call_action('package_search', {}, fq=fq)
    #     eq_(new_results['count'], 1)

    #     new_dataset = new_results['results'][0]
    #     new_resource = new_dataset.get('resources')[0]

    #     eq_(existing_resource['name'], 'Example resource 1')
    #     eq_(len(new_dataset.get('resources')), 1)
    #     eq_(new_resource['name'], 'Example resource 1 (updated)')
    #     return (existing_resource, new_resource)

    # def test_harvest_delete_rdf(self):

    #     self._test_harvest_delete(self.rdf_mock_url,
    #                               self.rdf_content,
    #                               self.rdf_remote_file_small,
    #                               self.rdf_content_type)

    # def test_harvest_delete_ttl(self):

    #     self._test_harvest_delete(self.ttl_mock_url,
    #                               self.ttl_content,
    #                               self.ttl_remote_file_small,
    #                               self.ttl_content_type)

    # def _test_harvest_delete(self, url, content, content_small, content_type):

    #     # Mock the GET request to get the file
    #     httpretty.register_uri(httpretty.GET, url,
    #                            body=content, content_type=content_type)

    #     # The harvester will try to do a HEAD request first so we need to mock
    #     # this as well
    #     httpretty.register_uri(httpretty.HEAD, url,
    #                            status=405, content_type=content_type)

    #     harvest_source = self._create_harvest_source(url)

    #     # First run, will create two datasets as previously tested
    #     self._run_full_job(harvest_source['id'], num_objects=2)

    #     # Run the jobs to mark the previous one as Finished
    #     self._run_jobs()

    #     # Mock a deletion in the remote file
    #     httpretty.register_uri(httpretty.GET, url,
    #                            body=content_small, content_type=content_type)

    #     # Run a second job
    #     self._run_full_job(harvest_source['id'], num_objects=2)

    #     # Check that we only have one dataset
    #     fq = "+type:dataset harvest_source_id:{0}".format(harvest_source['id'])
    #     results = h.call_action('package_search', {}, fq=fq)

    #     eq_(results['count'], 1)

    #     eq_(results['results'][0]['title'], 'Example dataset 1')

    # def test_harvest_bad_format_rdf(self):

    #     self._test_harvest_bad_format(self.rdf_mock_url,
    #                                   self.rdf_remote_file_invalid,
    #                                   self.rdf_content_type)

    # def test_harvest_bad_format_ttl(self):

    #     self._test_harvest_bad_format(self.ttl_mock_url,
    #                                   self.ttl_remote_file_invalid,
    #                                   self.ttl_content_type)

    # def _test_harvest_bad_format(self, url, bad_content, content_type):

    #     # Mock the GET request to get the file
    #     httpretty.register_uri(httpretty.GET, url,
    #                            body=bad_content, content_type=content_type)

    #     # The harvester will try to do a HEAD request first so we need to mock
    #     # this as well
    #     httpretty.register_uri(httpretty.HEAD, url,
    #                            status=405, content_type=content_type)

    #     harvest_source = self._create_harvest_source(url)
    #     self._create_harvest_job(harvest_source['id'])
    #     self._run_jobs(harvest_source['id'])
    #     self._gather_queue(1)

    #     # Run the jobs to mark the previous one as Finished
    #     self._run_jobs()

    #     # Get the harvest source with the udpated status
    #     harvest_source = h.call_action('harvest_source_show',
    #                                    id=harvest_source['id'])

    #     last_job_status = harvest_source['status']['last_job']

    #     eq_(last_job_status['status'], 'Finished')
    #     assert ('Error parsing the RDF file'
    #             in last_job_status['gather_error_summary'][0][0])
