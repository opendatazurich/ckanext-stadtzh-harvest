# coding: utf-8

import os, re
import datetime
import difflib
import shutil
import traceback
import uuid
from contextlib import contextmanager
from lxml import etree
from cgi import FieldStorage
from pylons import config
from ckan import model
from ckan.model import Session
from ckan.logic import action, get_action, NotFound
import ckan.plugins.toolkit as tk
from ckan import plugins as p
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.stadtzhtheme.plugin import StadtzhThemePlugin

import logging
log = logging.getLogger(__name__)


class InvalidCommentError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


@contextmanager
def retry_open_file(path, mode, tries=10, close=True):
    """
    This file-opening context manager is needed for flaky WebDAV connections
    We randomly get "IOError: [Errno 5] Input/output error", therefore this cm
    simply retries to open the file several times.
    The `close` parameter is needed for the cgi.FieldStorage, which requires an 
    open file handle
    """
    error = None
    while tries:
	try:
	    the_file = open(path, mode)
	except IOError as e:
	    error = e
	    tries -= 1
            log.exception("Error occured when opening %s: %r (tries left: %s)" % (path, e, tries))
	else:
	    break
    if not tries:
        if not the_file.closed:
            the_file.close()
	raise error
    yield the_file
    if close:
        the_file.close()


class StadtzhHarvester(HarvesterBase):
    '''
    Harvester for the City of Zurich
    '''

    ORGANIZATION = {
        'de': u'Stadt Zürich',
        'fr': u'fr_Stadt Zürich',
        'it': u'it_Stadt Zürich',
        'en': u'en_Stadt Zürich',
    }

    def __init__(self, **kwargs):
        HarvesterBase.__init__(self, **kwargs)
        try:
            self.CKAN_SITE_URL = config['ckan.site_url']
            self.DIFF_PATH = config['metadata.diffpath']
        except KeyError as e:
            raise Exception("'%s' not found in config" % e.message)

    def info(self):
        return {
            'name': 'stadtzh_harvester',
            'title': 'Harvester for the City of Zurich',
            'description': 'Harvester for the DWH and GEO dropzones of the City of Zurich'
        }

    def validate_config(self, config_str):
        config_obj = json.loads(config_str)
        self._validate_string_config(config_obj, 'data_path', required=True)
        self._validate_string_config(config_obj, 'metadata_dir', required=True)
        self._validate_string_config(config_obj, 'metafile_dir')
        self._validate_boolean_config(config_obj, 'update_datasets')

        return config_str
    
    def _validate_string_config(self, source, field, required=False):
        if field in source:
            value = source[field]
            if not isinstance(value, basestring):
                raise ValueError('%s must be a string' % field)
        elif required:
            raise ValueError('%s is required' % field)

    def _validate_boolean_config(self, source, field, required=True):
        if field in source:
            value = source[field]
            if not isinstance(value, bool):
                raise ValueError('%s must be a boolean' % field)
        elif required:
            raise ValueError('%s is required' % field)

    def _set_config(self, config_str):
        self.config = json.loads(config_str)

        if 'user' not in self.config:
            self.config['user'] = 'harvest'
        if 'metafile_dir' not in self.config:
            self.config['metafile_dir'] = ''
        if 'update_datasets' not in self.config:
            self.config['update_datasets'] = False

        log.debug('Using config: %r' % self.config)

    def gather_stage(self, harvest_job):
        log.debug('In StadtzhHarvester gather_stage')
        self._set_config(harvest_job.source.config)

        ids = []
        try:
            # list directories in dropzone folder
            datasets = self._remove_hidden_files(os.listdir(self.config['data_path']))
            log.debug("Directories in %s: %s" % (self.config['data_path'], datasets))

            # foreach -> meta.xml -> create entry
            for dataset in datasets:
                log.debug(self._validate_package_id(dataset))
                dataset_id = self._validate_package_id(dataset)
                if dataset_id:
                    meta_xml_file_path = os.path.join(self.config['data_path'], dataset, self.config['metafile_dir'], 'meta.xml')
                    if os.path.exists(meta_xml_file_path):
                        try:
                            with retry_open_file(meta_xml_file_path, 'r') as meta_xml:
                                parser = etree.XMLParser(encoding='utf-8')
                                dataset_node = etree.fromstring(meta_xml.read(), parser=parser).find('datensatz')
                            metadata = self._dropzone_get_metadata(dataset_id, dataset, dataset_node)
                        except Exception, e:
                            log.exception(e)
                            self._save_gather_error(
                                'Could not parse metadata in %s: %s / %s'
                                % (meta_xml_file_path, str(e), traceback.format_exc()),
                                harvest_job
                            )
                            continue
                    else:
                        metadata = {
                            'datasetID': dataset_id,
                            'datasetFolder': dataset,
                            'title': dataset,
                            'url': None,
                            'resources': self._generate_resources_dict_array(dataset),
                        }
                    if not os.path.isdir(os.path.join(self.DIFF_PATH, self.config['metadata_dir'], dataset_id)):
                        os.makedirs(os.path.join(self.DIFF_PATH, self.config['metadata_dir'], dataset_id))

                    with open(os.path.join(self.DIFF_PATH, self.config['metadata_dir'], dataset_id, 'metadata-' + str(datetime.date.today())), 'w') as meta_json:
                        meta_json.write(json.dumps(metadata, sort_keys=True, indent=4, separators=(',', ': ')))
                        log.debug('Metadata JSON created')

                    id = self._save_harvest_object(metadata, harvest_job)
                    ids.append(id)

            self._create_notifications_for_deleted_datasets()
            return ids
        except Exception, e:
            log.exception(e)
            self._save_gather_error(
                'Unable to get content from folder: %s: %s / %s'
                % (self.config['data_path'], str(e), traceback.format_exc()),
                harvest_job
            )
	    return []

    def fetch_stage(self, harvest_object):
        log.debug('In StadtzhHarvester fetch_stage')
        # Nothing to do here
        return True

    def import_stage(self, harvest_object):
        log.debug('In StadtzhHarvester import_stage')
        self._set_config(harvest_object.job.source.config)

        if not harvest_object:
            log.error('No harvest object received')
            self._save_object_error(
                'No harvest object received',
                harvest_object
            )
            return False

        try:
            self._import_package(harvest_object)
            Session.commit()
            return True
        except Exception, e:
            log.exception(e)
            self._save_object_error(
                (
                    'Unable to get content for package: %s: %r / %s'
                    % (harvest_object.guid, e, traceback.format_exc())
                ),
                harvest_object
            )
            return False

    def _import_package(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        package_dict['id'] = harvest_object.guid
        package_dict['name'] = munge_title_to_name(package_dict[u'datasetID'])
        context = {
            'model': model,
            'session': Session,
            'user': self.config['user']
        }

        # check if package already exists and migrate old packages to new ones if needed
        try:
            existing_package = get_action('package_show')(context.copy(), {'id': package_dict['id']})
        except NotFound:
            existing_package = None
            log.debug('Could not find pkg %s' % package_dict['name'])
                

        # update existing resources, delete old ones, create new ones
        action_dict = {} 
        new_resources = self._generate_resources_dict_array(package_dict['datasetFolder'], include_files=True)
        if not existing_package:
            for r in new_resources:
                action_dict[r['name']] = {'action': 'create', 'new_resource': r}
        else:
            old_resources = existing_package['resources']
            for r in new_resources:
                action = {'action': 'create', 'new_resource': r, 'old_resource': None}
                for old in old_resources:
                    if old['name'] == r['name']:
                        action['action'] = 'update'
                        action['old_resource'] = old
                        break
                action_dict[r['name']] = action

            for old in old_resources:
                if old['name'] not in action_dict:
                    action_dict[old['name']] = {'action': 'delete', 'old_resource': old}

        # Start the actions!
        if existing_package and 'resources' in existing_package: 
            package_dict['resources'] = existing_package['resources']

        self._find_or_create_organization(package_dict, context)

        # import the package if it does not yet exists (i.e. it's a new package)
        # or if this harvester is allowed to update packages
        if not existing_package:
            self._create_package(package_dict, harvest_object)
            self._create_notification_for_new_dataset(package_dict)
            log.debug('Dataset `%s` has been added' % package_dict['id'])
        else:
            # Don't change the dataset name even if the title has
            package_dict['name'] = existing_package['name']
            package_dict['id'] = existing_package['id']
            self._update_package(package_dict, harvest_object)
            log.debug('Dataset `%s` has been updated' % package_dict['id'])
        
        # create diffs if there is a previous package
        if existing_package:
            self._create_diffs(package_dict)

        # handle all resources (create, update, delete)
        for res_name, action in action_dict.iteritems():
            try:
                log.debug("Resource %s, action: %s" % (res_name, action))
                if action['action'] == 'create':
                    resource = dict(action['new_resource'])
                    resource['package_id'] = package_dict['id']
                    resource_id = get_action('resource_create')(context.copy(), resource)['id']
                    log.debug('Dataset resource `%s` has been created' % resource_id)
                elif action['action'] == 'update':
                    resource = dict(action['old_resource'])
                    resource['package_id'] = package_dict['id']

                    if 'upload' in action['new_resource']:
                        # if the resource is an upload, replace the file
                        resource['upload'] = action['new_resource']['upload']
                    elif action['new_resource']['resource_type'] == 'api':
                        # for APIs, update the URL
                        resource['url'] = action['new_resource']['url']

                    log.debug("Trying to update resource: %s" % resource)
                    resource_id = get_action('resource_update')(context.copy(), resource)['id']
                    log.debug('Dataset resource `%s` has been updated' % resource_id)
                elif action['action'] == 'delete':
                    replace_upload = get_action('resource_update')(
                        context.copy(),
                        {
                            'id': action['old_resource']['id'],
                            'url': 'https://data.stadt-zuerich.ch/filenotfound',
                            'clear_upload': 'true'
                        }
                    )
                    result = get_action('resource_delete')(context.copy(), {'id': action['old_resource']['id']})
                    log.debug('Dataset resource has been deleted: %s' % result)
                else:
                    raise ValueError('Unknown action, we should never reach this point')
            except Exception, e:
                self._save_object_error('Error while handling action %s for resource %s in pkg %s: %r %s' % (action, res_name, package_dict['name'], e, traceback.format_exc()), harvest_object, 'Import')
                continue
        Session.commit()
        return True


    def _create_package(self, dataset, harvest_object):
        theme_plugin = StadtzhThemePlugin()
        package_schema = theme_plugin.create_package_schema()
    
        # We need to explicitly provide a package ID
        dataset['id'] = unicode(uuid.uuid4())
        package_schema['id'] = [unicode]
        
        context = {
            'user': self.config['user'],
            'return_id_only': True,
            'ignore_auth': True,
            'schema': package_schema,
        }

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()
    
        # Save reference to the package on the object
        harvest_object.package_id = dataset['id']
        harvest_object.add()
    
        # Defer constraints and flush so the dataset can be indexed with
        # the harvest object id (on the after_show hook from the harvester
        # plugin)
        model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
        model.Session.flush()
    
        try:
            p.toolkit.get_action('package_create')(context, dataset)
        except p.toolkit.ValidationError, e:
            self._save_object_error('Create validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
            return False
    
        log.info('Created dataset %s', dataset['name'])
        
        model.Session.commit()
        
        return True

    def _update_package(self, dataset, harvest_object):
        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
                                       .filter(HarvestObject.guid==harvest_object.guid) \
                                       .filter(HarvestObject.current==True) \
                                       .first()
        
        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        # Save reference to the package on the object
        harvest_object.package_id = dataset['id']
        harvest_object.add()

        # only update pkg if this harvester allows it
        if self.config['update_datasets']:
            theme_plugin = StadtzhThemePlugin()
            context = {
                'user': self.config['user'],
                'return_id_only': True,
                'ignore_auth': True,
                'schema': theme_plugin.update_package_schema(),
            }
            try:
                get_action('package_update')(context, dataset)
            except p.toolkit.ValidationError, e:
                self._save_object_error('Update validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False
            log.info('Updated dataset %s', dataset['name'])
        else:
            log.info('Dataset %s *not* updated because update_datasets config is set to `false`', dataset['name'])

        model.Session.commit()
        return True

    def _save_harvest_object(self, metadata, harvest_job):
        '''
        Save the harvest object with the given metadata dict and harvest_job
        '''

        obj = HarvestObject(
            guid=metadata['datasetID'],
            job=harvest_job,
            content=json.dumps(metadata)
        )
        obj.save()
        log.debug('adding ' + metadata['datasetID'] + ' to the queue')

        return obj.id

    def _get_group_ids(self, group_list):
        '''
        Return the group ids for the given groups.
        The list should contain group tuples: (name, title)
        If a group does not exist in CKAN, create it.
        '''

        context = {
            'model': model,
            'session': Session,
            'ignore_auth': True,
            'user': self.config['user']
        }
        groups = []
        for name, title in group_list:
            data_dict = {'id': name}
            try:
                group_id = get_action('group_show')(context, data_dict)['id']
                groups.append({'name': group_id})
                log.debug('Added group %s' % name)
            except:
                data_dict['name'] = name
                data_dict['title'] = title
                data_dict['image_url'] = self.CKAN_SITE_URL + '/kategorien/' + name + '.png'
                log.debug('Couldn\'t get group id. Creating the group `%s` with data_dict: %s', name, data_dict)
                try:
                    group = get_action('group_create')(context, data_dict)
                    log.debug("Created group %s" % group)
                    groups.append({'name': group['id']})
                except:
                    log.debug('Couldn\'t create group: %s' % (traceback.format_exc()))
                    raise

        return groups

    def _dropzone_get_groups(self, dataset_node):
        '''
        Get the groups from the node, normalize them and get the ids.
        '''
        categories = self._get(dataset_node, 'kategorie')
        if categories:
            group_titles = categories.split(', ')
            groups = []
            for title in group_titles:
                name = munge_title_to_name(title)
                groups.append((name, title))
            return self._get_group_ids(groups)
        else:
            return []

    def _dropzone_get_metadata(self, dataset_id, dataset_folder, dataset_node):
        '''
        For the given dataset node return the metadata dict.
        '''

        return {
            'datasetID': dataset_id,
            'datasetFolder': dataset_folder,
            'title': dataset_node.find('titel').text,
            'url': self._get(dataset_node, 'lieferant'),
            'notes': dataset_node.find('beschreibung').text,
            'author': dataset_node.find('quelle').text,
            'maintainer': 'Open Data Zürich',
            'maintainer_email': 'opendata@zuerich.ch',
            'license_id': 'cc-zero',
            'license_url': 'http://opendefinition.org/licenses/cc-zero/',
            'tags': self._generate_tags(dataset_node),
            'groups': self._dropzone_get_groups(dataset_node),
            'spatialRelationship': self._get(dataset_node, 'raeumliche_beziehung'),
            'dateFirstPublished': self._get(dataset_node, 'erstmalige_veroeffentlichung'),
            'dateLastUpdated': self._get(dataset_node, 'aktualisierungsdatum'),
            'updateInterval': self._get_update_interval(dataset_node),
            'dataType': self._get_data_type(dataset_node),
            'legalInformation': self._get(dataset_node, 'rechtsgrundlage'),
            'version': self._get(dataset_node, 'aktuelle_version'),
            'timeRange': self._get(dataset_node, 'zeitraum'),
            'sszBemerkungen': self._convert_comments(dataset_node),
            'sszFields': self._json_encode_attributes(self._get_attributes(dataset_node)),
            'dataQuality': self._get(dataset_node, 'datenqualitaet'),
        }

    def _get_update_interval(self, dataset_node):
        interval = self._get(dataset_node, 'aktualisierungsintervall').replace(u'ä', u'ae').replace(u'ö', u'oe').replace(u'ü', u'ue')
        if not interval:
            return '   '
        return interval

    def _get_data_type(self, dataset_node):
        data_type = self._get(dataset_node, 'datentyp')
        if not data_type:
            return '   '
        return data_type

    def _remove_hidden_files(self, file_list):
        '''
        Removes dotfiles from a list of files
        '''
        cleaned_file_list = []
        for file in file_list:
            if not file.startswith('.'):
                cleaned_file_list.append(file)
        return cleaned_file_list

    def _generate_tags(self, dataset_node):
        '''
        Given a dataset node it extracts the tags and returns them in an array
        '''
        tags = []
        if dataset_node.find('schlagworte') is not None and dataset_node.find('schlagworte').text:
            for tag in dataset_node.find('schlagworte').text.split(', '):
                tags.append({'name': tag})
        log.debug('Added tags: %s' % str(tags))
        return tags

    def _sort_resource(self, x, y):

        order = {
            'zip':  1,
            'wms':  2,
            'wfs':  3,
            'kmz':  4,
            'json': 5
        }

        x_format = x['format'].lower()
        y_format = y['format'].lower()
        if x_format not in order:
            return -1
        if y_format not in order:
            return 1
        return cmp(order[x_format], order[y_format])

    def _generate_resources_dict_array(self, dataset, include_files=False):
        '''
        Given a dataset folder, it'll return an array of resource metadata
        '''
        resources = []
        resource_files = self._remove_hidden_files((f for f in os.listdir(os.path.join(self.config['data_path'], dataset, self.config['metafile_dir']))
                                                    if os.path.isfile(os.path.join(self.config['data_path'], dataset, self.config['metafile_dir'], f))))
        log.debug(resource_files)

        # for resource_file in resource_files:
        for resource_file in (x for x in resource_files if x != 'meta.xml'):
            resource_path = os.path.join(self.config['data_path'], dataset, self.config['metafile_dir'], resource_file)
            if resource_file == 'link.xml':
                with retry_open_file(resource_path, 'r') as links_xml:
                    parser = etree.XMLParser(encoding='utf-8')
                    links = etree.fromstring(links_xml.read(), parser=parser).findall('link')

                    for link in links:
                        if link.find('url').text != "" and link.find('url').text is not None:
                            resources.append({
                                'url': link.find('url').text,
                                'name': link.find('lable').text,
                                'format': link.find('type').text,
                                'resource_type': 'api'
                            })
            else:
                resource_file = self._validate_filename(resource_file)
                if resource_file:
                    resource_dict = {
                        'name': resource_file,
                        'url': '',
                        'format': resource_file.split('.')[-1],
                        'resource_type': 'file'
                    }
                    if include_files:
                        with retry_open_file(resource_path, 'r', close=False) as f:
                            field_storage = FieldStorage()
                            field_storage.file = f
                            field_storage.filename = f.name
                            resource_dict['upload'] = field_storage
                    resources.append(resource_dict)

        sorted_resources = sorted(resources, cmp=lambda x, y: self._sort_resource(x, y))
        return sorted_resources

    def _node_exists_and_is_nonempty(self, dataset_node, element_name):
        element = dataset_node.find(element_name)
        if element is None:
            return None
        elif element.text is None:
            return None
        else:
            return element.text

    def _get(self, node, name):
        element = self._node_exists_and_is_nonempty(node, name)
        if element:
            return element
        else:
            return ''

    def _convert_comments(self, node):
        comments = node.find('bemerkungen')
        if comments is not None:
            log.debug(comments.tag + ' ' + str(comments.attrib))
            markdown = ''
            for comment in comments:
                if self._get(comment, 'titel'):
                    markdown += '**' + self._get(comment, 'titel') + '**\n\n'
                if self._get(comment, 'text'):
                    markdown += self._get(comment, 'text') + '\n\n'
                link = comment.find('link')
                if link is not None:
                    label = self._get(link, 'label')
                    url = self._get(link, 'url')
                    markdown += '[' + label + '](' + url + ')\n\n'
            try:
                return self._validate_comment(markdown)
            except InvalidCommentError:
                return ''

    def _json_encode_attributes(self, properties):
        attributes = []
        for key, value in properties:
            if value:
                attributes.append((key, value))

        return json.dumps(attributes)

    def _get_attributes(self, node):
        attribut_list = node.find('attributliste')
        attributes = []
        for attribut in attribut_list:
            tech_name = attribut.get('technischerfeldname')
            speak_name = attribut.find('sprechenderfeldname').text
            attributes.append(('%s (technisch: %s)' % (speak_name, tech_name), attribut.find('feldbeschreibung').text))
        return attributes

    def _get_immediate_subdirectories(self, directory):
        return [name for name in os.listdir(directory)
                if os.path.isdir(os.path.join(directory, name))]

    def _diff_path(self, package_id):
        today = datetime.date.today()
        package_id = self._validate_package_id(package_id)
        if package_id:
            return os.path.join(self.DIFF_PATH, str(today) + '-' + package_id + '.html')

    def _create_notifications_for_deleted_datasets(self):
        current_datasets = self._get_immediate_subdirectories(self.config['data_path'])
        cached_datasets = self._get_immediate_subdirectories(os.path.join(self.DIFF_PATH, self.config['metadata_dir']))
        for package_id in cached_datasets:
            # Validated package_id can only contain alphanumerics and underscores
            package_id = self._validate_package_id(package_id)
            if package_id and package_id not in current_datasets:
                log.debug('Dataset `%s` has been deleted' % package_id)
                # delete the metadata directory
                metadata_dir = os.path.join(self.DIFF_PATH, self.config['metadata_dir'], package_id)
                log.debug('Removing metadata dir `%s`' % metadata_dir)
                shutil.rmtree(metadata_dir)
                # only send notification if there is a package in CKAN
                if model.Package.get(package_id):
                    path = self._diff_path(package_id)
                    with open(path, 'w') as deleted_info:
                        deleted_info.write(
                            "<!DOCTYPE html>\n<html>\n<body>\n<h2>Dataset deleted: <a href=\""
                            + self.CKAN_SITE_URL + "/dataset/" + package_id + "\">"
                            + package_id + "</a></h2></body></html>\n"
                        )
                    log.debug('Wrote deleted notification to file `%s`' % path)

    def _create_notification_for_new_dataset(self, package_dict):
        # Validated package_id can only contain alphanumerics and underscores
        package_id = self._validate_package_id(package_dict['id'])
        if package_id:
            path = self._diff_path(package_id)
            with open(path, 'w') as new_info:
                new_info.write(
                    "<!DOCTYPE html>\n<html>\n<body>\n<h2>New dataset added: <a href=\""
                    + self.CKAN_SITE_URL + "/dataset/" + package_id + "\">"
                    + package_id + "</a></h2></body></html>\n"
                )
            log.debug('Wrote added dataset notification to file `%s`' % path)


    def _create_diffs(self, package_dict):
        try:
            # Validated package_id can only contain alphanumerics and underscores
            package_id = self._validate_package_id(package_dict['id'])
            if not package_id:
                raise ValueError("Package ID '%s' is not valid" % package_dict['id'])
            new_metadata_path = os.path.join(self.DIFF_PATH, self.config['metadata_dir'], package_id)
            prev_metadata_path = os.path.join(self.DIFF_PATH, self.config['metadata_dir'], package_id)
            new_metadata_file = os.path.join(new_metadata_path, 'metadata-' + str(datetime.date.today()))
            prev_metadata_file = os.path.join(new_metadata_path, 'metadata-previous')

            for path in [self.DIFF_PATH, new_metadata_path, prev_metadata_path]:
                if not os.path.isdir(path):
                    os.makedirs(path)

            if not os.path.isfile(new_metadata_file):
                log.debug(new_metadata_file + ' Metadata JSON missing for the dataset: ' + package_id)
                with open(new_metadata_file, 'w') as new_metadata:
                    new_metadata.write('')
                log.debug('Created new empty metadata file.')

            if not os.path.isfile(prev_metadata_file):
                log.debug('No earlier metadata JSON for the dataset: ' + package_id)
                with open(prev_metadata_file, 'w') as prev_metadata:
                    prev_metadata.write('')
                log.debug('Created new empty metadata file.')

            with open(prev_metadata_file) as prev_metadata:
                with open(new_metadata_file) as new_metadata:
                    prev = prev_metadata.read()
                    new = new_metadata.read()

            if prev == new:
                log.debug('No change in metadata for the dataset: ' + package_id)
            else:
                with open(prev_metadata_file) as prev_metadata:
                    with open(new_metadata_file) as new_metadata:
                        with open(self._diff_path(package_id), 'w') as diff:
                            diff.write(
                                "<!DOCTYPE html>\n<html>\n<body>\n<h2>Metadata diff for the dataset <a href=\""
                                + self.CKAN_SITE_URL + "/dataset/" + package_id + "\">"
                                + package_id + "</a></h2></body></html>\n"
                            )
                            d = difflib.HtmlDiff(wrapcolumn=60)
                            umlauts = {
                                "\\u00e4": "ä",
                                "\\u00f6": "ö",
                                "\\u00fc": "ü",
                                "\\u00c4": "Ä",
                                "\\u00d6": "Ö",
                                "\\u00dc": "Ü",
                                "ISO-8859-1": "UTF-8"
                            }
                            html = d.make_file(prev_metadata, new_metadata, context=True, numlines=1)
                            for code in umlauts.keys():
                                html = html.replace(code, umlauts[code])
                            diff.write(html)
                            log.debug('Metadata diff generated for the dataset: ' + package_id)

            os.remove(prev_metadata_file)
            log.debug('Deleted previous day\'s metadata file.')
            os.rename(new_metadata_file, prev_metadata_file)
        except AttributeError:
            pass

    def _find_or_create_organization(self, package_dict, context):
        # Find or create the organization the dataset should get assigned to.
        try:
            data_dict = {
                'permission': 'edit_group',
                'id': munge_title_to_name(self.ORGANIZATION['de']),
                'name': munge_title_to_name(self.ORGANIZATION['de']),
                'title': self.ORGANIZATION['de']
            }
            package_dict['owner_org'] = get_action('organization_show')(context, data_dict)['id']
        except:
            organization = get_action('organization_create')(context, data_dict)
            package_dict['owner_org'] = organization['id']

    def _validate_package_id(self, package_id):
        # Validate that they do not contain any HTML tags.
        match = re.search('[<>]+', package_id)
        if match:
            log.debug('Package id %s contains disallowed characters' % package_id)
            return False
        else:
            return munge_title_to_name(package_id)

    def _validate_filename(self, filename):
        # Validate that they do not contain any HTML tags.
        match = re.search('[<>]+', filename)
        if len(filename) == 0:
            log.debug('Filename is empty.')
            return False
        if match:
            log.debug('Filename %s not added as it contains disallowed characters' % filename)
            return False
        else:
            return filename

    def _validate_comment(self, markdown):
        # Validate that they do not contain any HTML tags.
        match = re.search('[<>]+', markdown)
        if len(markdown) == 0:
            log.debug('Comment is empty.')
            return ''
        if match:
            log.debug('Comment not added as it contains disallowed characters: %s' % markdown)
            raise InvalidCommentError(markdown)
        else:
            return markdown
