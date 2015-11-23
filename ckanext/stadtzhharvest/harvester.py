# coding: utf-8

import os, re
import datetime
import difflib
import shutil
import traceback
from lxml import etree
from ofs import get_impl
from pylons import config
from ckan import model
from ckan.model import Session
from ckan.logic import action, get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name, substitute_ascii_equivalents
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

import logging
log = logging.getLogger(__name__)


class InvalidCommentError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class StadtzhHarvester(HarvesterBase):
    '''
    BaseHarvester for the City of Zurich
    '''

    ORGANIZATION = {
        'de': u'Stadt Zürich',
        'fr': u'fr_Stadt Zürich',
        'it': u'it_Stadt Zürich',
        'en': u'en_Stadt Zürich',
    }
    LANG_CODES = ['de', 'fr', 'it', 'en']

    config = {
        'user': u'harvest'
    }

    META_DIR = ''
    BUCKET = 'default'

    def __init__(self, **kwargs):
        HarvesterBase.__init__(self, **kwargs)
        try:
            self.INTERNAL_SITE_URL = config['ckan.site_url_internal']
            self.CKAN_SITE_URL = config['ckan.site_url']
            self.DIFF_PATH = config['metadata.diffpath']
        except KeyError as e:
            raise Exception("'%s' not found in config" % e.message)

    def gather_stage(self, harvest_job):
        raise NotImplementedError("This is only a base harvester, use one of its childs") 

    def fetch_stage(self, harvest_object):
        raise NotImplementedError("This is only a base harvester, use one of its childs") 

    def import_stage(self, harvest_object):
        raise NotImplementedError("This is only a base harvester, use one of its childs") 

    def _gather_datasets(self, harvest_job):
        ids = []
        try:
            # list directories in dropzone folder
            datasets = self._remove_hidden_files(os.listdir(self.DATA_PATH))

            # foreach -> meta.xml -> create entry
            for dataset in datasets:
                log.debug(self._validate_package_id(dataset))
                dataset_id = self._validate_package_id(dataset)
                if dataset_id:
                    meta_xml_file_path = os.path.join(self.DATA_PATH, dataset, self.META_DIR, 'meta.xml')
                    if os.path.exists(meta_xml_file_path):
                        try:
                            with open(meta_xml_file_path, 'r') as meta_xml:
                                parser = etree.XMLParser(encoding='utf-8')
                                dataset_node = etree.fromstring(meta_xml.read(), parser=parser).find('datensatz')
                            metadata = self._dropzone_get_metadata(dataset, dataset_node)
                        except:
                            continue
                    else:
                        metadata = {
                            'datasetID': dataset_id,
                            'title': dataset,
                            'url': None,
                            'resources': self._generate_resources_dict_array(dataset),
                            'related': []
                        }
                    if not os.path.isdir(os.path.join(self.DIFF_PATH, self.METADATA_DIR, dataset)):
                        os.makedirs(os.path.join(self.DIFF_PATH, self.METADATA_DIR, dataset))

                    with open(os.path.join(self.DIFF_PATH, self.METADATA_DIR, dataset, 'metadata-' + str(datetime.date.today())), 'w') as meta_json:
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
                % (self.DATA_PATH, str(e), traceback.format_exc()),
                harvest_job
            )
	    return []

    def _fetch_datasets(self, harvest_object):
        if not harvest_object:
            log.error('No harvest object received')
            self._save_object_error(
                'No harvest object received',
                harvest_object
            )
            return False
        # Get the URL
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        try:
            harvest_object.save()
            log.debug('successfully processed ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)
            self._save_object_error(
                (
                    'Unable to get content for package: %s: %r / %s'
                    % (datasetID, e, traceback.format_exc())
                ),
                harvest_object
            )
            return False

    def _import_datasets(self, harvest_object):
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

        user = model.User.get(self.config['user'])
        context = {
            'model': model,
            'session': Session,
            'user': self.config['user']
        }

        self._find_or_create_organization(package_dict, context)

        # Always update files of resources in filestore
        self._add_resources_to_filestore(package_dict)

        # import the package if it does not yet exists (i.e. it's a new package)
        # or if this harvester is allowed to update packages
        package = model.Package.get(package_dict['id'])
        if not package or self._import_updated_packages():
            result = self._create_or_update_package(package_dict, harvest_object)
            log.debug('Dataset `%s` has been added or updated' % package_dict['id'])

        # Update "Related items" only the first time, do not update via harvester
        if not package:
            self._related_create_or_update(package_dict['name'], package_dict['related'])

        if package:
            # package has already been imported.
            try:
                self._create_diffs(package_dict)
            except AttributeError:
                pass
        else:
            self._create_notification_for_new_dataset(package_dict)

    def _import_updated_packages(self):
        '''
        Define wheter packages may be updated automatically using this harvester.
        If not, only new packages are imported.
        This method should be overridden in sub-classes accordingly
        '''
        return False

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

        user = model.User.get(self.config['user'])
        context = {
            'model': model,
            'session': Session,
            'user': self.config['user']
        }
        groups = []
        for name, title in group_list:
            try:
                data_dict = {'id': name}
                group_id = get_action('group_show')(context, data_dict)['id']
                groups.append(group_id)
                log.debug('Added group %s' % name)
            except:
                data_dict['name'] = name
                data_dict['title'] = title
                data_dict['image_url'] = self.CKAN_SITE_URL + '/kategorien/' + name + '.png'
                log.debug('Couldn\'t get group id. Creating the group `%s` with data_dict: %s', name, data_dict)
                group_id = get_action('group_create')(context, data_dict)['id']
                groups.append(group_id)

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
                if title == u'Bauen und Wohnen':
                    name = u'bauen-wohnen'
                else:
                    name = title.lower().replace(u'ö', u'oe').replace(u'ä', u'ae')
                groups.append((name, title))
            return self._get_group_ids(groups)
        else:
            return []

    def _dropzone_get_metadata(self, dataset_id, dataset_node):
        '''
        For the given dataset node return the metadata dict.
        '''

        return {
            'datasetID': dataset_id,
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
            'resources': self._generate_resources_dict_array(dataset_id),
            'extras': [
                ('spatialRelationship', self._get(dataset_node, 'raeumliche_beziehung')),
                ('dateFirstPublished', self._get(dataset_node, 'erstmalige_veroeffentlichung')),
                ('dateLastUpdated', self._get(dataset_node, 'aktualisierungsdatum')),
                ('updateInterval', self._get_update_interval(dataset_node)),
                ('dataType', self._get_data_type(dataset_node)),
                ('legalInformation', self._get(dataset_node, 'rechtsgrundlage')),
                ('version', self._get(dataset_node, 'aktuelle_version')),
                ('timeRange', self._get(dataset_node, 'zeitraum')),
                ('sszBemerkungen', self._convert_comments(dataset_node)),
                ('sszFields', self._json_encode_attributes(self._get_attributes(dataset_node))),
                ('dataQuality', self._get(dataset_node, 'datenqualitaet'))
            ],
            'related': self._get_related(dataset_node)
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
                tags.append(tag)
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

    def _generate_resources_dict_array(self, dataset):
        '''
        Given a dataset folder, it'll return an array of resource metadata
        '''
        resources = []
        resource_files = self._remove_hidden_files((f for f in os.listdir(os.path.join(self.DATA_PATH, dataset, self.META_DIR))
                                                    if os.path.isfile(os.path.join(self.DATA_PATH, dataset, self.META_DIR, f))))
        log.debug(resource_files)

        # for resource_file in resource_files:
        for resource_file in (x for x in resource_files if x != 'meta.xml'):
            if resource_file == 'link.xml':
                with open(os.path.join(self.DATA_PATH, dataset, self.META_DIR, resource_file), 'r') as links_xml:
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
                    resources.append({
                        # 'url': '', # will be filled in the import stage
                        'name': resource_file,
                        'format': resource_file.split('.')[-1],
                        'resource_type': 'file'
                    })

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

    def _get_related(self, xpath):
        related = []
        for element, related_type in [('anwendungen', 'Applikation'),
                                      ('publikationen', 'Publikation')]:
            related_list = xpath.find(element)
            if related_list is not None:
                for item in related_list:
                    title = self._get(item, 'titel')
                    description = self._get(item, 'beschreibung')
                    # title is mandatory so use description if it is empty
                    if not title:
                        title = description
                    related.append({
                        'title': title,
                        'type': related_type,
                        'description': description,
                        'url': self._get(item, 'url')
                    })
        return related

    def _related_create_or_update(self, dataset_id, data):
        context = {
            'model': model,
            'session': Session,
            'user': self.config['user']
        }

        related_items = {}
        data_dict = {
            'id': dataset_id
        }
        for related in action.get.related_list(context, data_dict):
            related_items[related['url']] = related

        for entry in data:
            entry['dataset_id'] = dataset_id
            if entry['url'] in related_items.keys():
                entry = dict(related_items[entry['url']].items() + entry.items())
                log.debug('Updating related %s' % entry)
                action.update.related_update(context, entry)
            else:
                try:
                    log.debug('Creating related %s' % entry)
                    action.create.related_create(context, entry)
                except Exception, e:
                    log.exception(e)

    def _get_immediate_subdirectories(self, directory):
        return [name for name in os.listdir(directory)
                if os.path.isdir(os.path.join(directory, name))]

    def _diff_path(self, package_id):
        today = datetime.date.today()
        package_id = self._validate_package_id(package_id)
        if package_id:
            return os.path.join(self.DIFF_PATH, str(today) + '-' + package_id + '.html')

    def _create_notifications_for_deleted_datasets(self):
        current_datasets = self._get_immediate_subdirectories(self.DATA_PATH)
        cached_datasets = self._get_immediate_subdirectories(os.path.join(self.DIFF_PATH, self.METADATA_DIR))
        for package_id in cached_datasets:
            # Validated package_id can only contain alphanumerics and underscores
            package_id = self._validate_package_id(package_id)
            if package_id and package_id not in current_datasets:
                log.debug('Dataset `%s` has been deleted' % package_id)
                # delete the metadata directory
                metadata_dir = os.path.join(self.DIFF_PATH, self.METADATA_DIR, package_id)
                log.debug('Removing metadata dir `%s`' % metadata_dir)
                shutil.rmtree(metadata_dir)
                # only send notification if there is a package in CKAN
                if model.Package.get(package_id):
                    path = self._diff_path(package_id)
                    with open(path, 'w') as deleted_info:
                        deleted_info.write(
                            "<!DOCTYPE html>\n<html>\n<body>\n<h2>Dataset deleted: <a href=\""
                            + self.INTERNAL_SITE_URL + "/dataset/" + package_id + "\">"
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
                    + self.INTERNAL_SITE_URL + "/dataset/" + package_id + "\">"
                    + package_id + "</a></h2></body></html>\n"
                )
            log.debug('Wrote added dataset notification to file `%s`' % path)


    def _create_diffs(self, package_dict):
        # Validated package_id can only contain alphanumerics and underscores
        package_id = self._validate_package_id(package_dict['id'])
        if package_id:
            new_metadata_path = os.path.join(self.DIFF_PATH, self.METADATA_DIR, package_id)
            prev_metadata_path = os.path.join(self.DIFF_PATH, self.METADATA_DIR, package_id)
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
                                + self.INTERNAL_SITE_URL + "/dataset/" + package_id + "\">"
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

    def _add_resources_to_filestore(self, package_dict):
        # Move file around and make sure it's in the file-store
        for r in package_dict['resources']:
            old_filename = self._validate_filename(r['name'])
            if not old_filename:
                log.debug('Resource not added.')
                continue
            r['name'] = substitute_ascii_equivalents(r['name'])
            package_id = self._validate_package_id(package_dict['datasetID'])

            if not package_id:
                log.debug('Resources not added to package %s as the package id contained disallowed characters' % package_id)

            if r['resource_type'] == 'file':
                label = package_id + '/' + r['name']
                file_contents = ''
                with open(os.path.join(self.DATA_PATH, package_id, self.META_DIR, old_filename)) as contents:
                    file_contents = contents.read()
                params = {
                    'filename-original': 'the original file name',
                    'uploaded-by': self.config['user']
                }
                r['url'] = self.CKAN_SITE_URL + '/storage/f/' + label
                self.get_ofs().put_stream(self.BUCKET, label, file_contents, params)

    def _validate_package_id(self, package_id):
        # Validate that they do not contain any HTML tags.
        match = re.search('[<>]+', package_id)
        if match:
            log.debug('Package id %s contains disallowed characters' % package_id)
            return False
        else:
            return package_id

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


    # ---
    # COPIED FROM THE CKAN STORAGE CONTROLLER
    # ---

    def create_pairtree_marker(self, folder):
        """ Creates the pairtree marker for tests if it doesn't exist """
        if not folder[:-1] == '/':
            folder = folder + '/'

        directory = os.path.dirname(folder)
        if not os.path.exists(directory):
            os.makedirs(directory)

        target = os.path.join(directory, 'pairtree_version0_1')
        if os.path.exists(target):
            return

        open(target, 'wb').close()

    def get_ofs(self):
        """Return a configured instance of the appropriate OFS driver.
        """
        storage_backend = config['ofs.impl']
        kw = {}
        for k, v in config.items():
            if not k.startswith('ofs.') or k == 'ofs.impl':
                continue
            kw[k[4:]] = v

        # Make sure we have created the marker file to avoid pairtree issues
        if storage_backend == 'pairtree' and 'storage_dir' in kw:
            self.create_pairtree_marker(kw['storage_dir'])

        ofs = get_impl(storage_backend)(**kw)
        return ofs

    # ---
    # END COPY
    # ---

