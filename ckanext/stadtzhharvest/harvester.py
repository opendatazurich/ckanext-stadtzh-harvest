# coding: utf-8

import os
import datetime
import difflib
from lxml import etree
from ofs import get_impl
from pylons import config
from ckan import model
from ckan.model import Session
from ckan.logic import action, get_action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name, munge_filename
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject

import logging
log = logging.getLogger(__name__)


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
    BUCKET = config.get('ckan.storage.bucket', 'default')
    CKAN_SITE_URL = config.get('ckan.site_url', 'http://example.com')

    config = {
        'user': u'harvest'
    }

    DIFF_PATH = config.get('metadata.diffpath', '/usr/lib/ckan/diffs')
    INTERNAL_SITE_URL = config.get('ckan.site_url_internal', 'http://internal.example.com')
    META_DIR = ''

    def _gather_datasets(self, harvest_job):

        ids = []

        # list directories in dropzone folder
        datasets = self._remove_hidden_files(os.listdir(self.DATA_PATH))

        # foreach -> meta.xml -> create entry
        for dataset in datasets:
            meta_xml_file_path = os.path.join(self.DATA_PATH, dataset, self.META_DIR, 'meta.xml')
            if os.path.exists(meta_xml_file_path):
                with open(meta_xml_file_path, 'r') as meta_xml:
                    parser = etree.XMLParser(encoding='utf-8')
                    dataset_node = etree.fromstring(meta_xml.read(), parser=parser).find('datensatz')
                metadata = self._dropzone_get_metadata(dataset, dataset_node)
            else:
                metadata = {
                    'datasetID': dataset,
                    'title': dataset,
                    'url': None,
                    'resources': self._generate_resources_dict_array(dataset),
                    'related': []
                }
            id = self._save_harvest_object(metadata, harvest_job)
            ids.append(id)

            if not os.path.isdir(os.path.join(self.METADATA_PATH, dataset)):
                os.makedirs(os.path.join(self.METADATA_PATH, dataset))

            with open(os.path.join(self.METADATA_PATH, dataset, 'metadata-' + str(datetime.date.today())), 'w') as meta_json:
                meta_json.write(json.dumps(metadata, sort_keys=True, indent=4, separators=(',', ': ')))
                log.debug('Metadata JSON created')

        return ids

    def _fetch_datasets(self, harvest_object):
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

    def _import_datasets(self, harvest_object):
        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            self._import_package(harvest_object)
            Session.commit()

        except Exception, e:
            log.exception(e)

        return True

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

        # Insert the package only when it's not already in CKAN, but move the resources anyway.
        package = model.Package.get(package_dict['id'])
        if package:
            # package has already been imported.
            try:
                self._create_diffs(package_dict)
            except AttributeError:
                pass
        else:
            # package does not exist, therefore create it.
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

        self._add_resources_to_filestore(package_dict)

        if not package:
            result = self._create_or_update_package(package_dict, harvest_object)
            self._related_create_or_update(package_dict['name'], package_dict['related'])

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
                ('comments', self._convert_comments(dataset_node)),
                ('attributes', self._json_encode_attributes(self._get_attributes(dataset_node))),
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
            if resource_file == u'link.xml':
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
            html = ''
            for comment in comments:
                if self._get(comment, 'titel'):
                    html += '**' + self._get(comment, 'titel') + '**\n\n'
                if self._get(comment, 'text'):
                    html += self._get(comment, 'text') + '\n\n'
                link = comment.find('link')
                if link is not None:
                    label = self._get(link, 'label')
                    url = self._get(link, 'url')
                    html += '[' + label + '](' + url + ')\n\n'
                return html

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
                    related.append({
                        'title': self._get(item, 'titel'),
                        'type': related_type,
                        'description': self._get(item, 'beschreibung'),
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

    def _create_diffs(self, package_dict):
        today = datetime.date.today()
        new_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-' + str(today))
        prev_metadata_path = os.path.join(self.METADATA_PATH, package_dict['id'], 'metadata-previous')
        diff_path = os.path.join(self.DIFF_PATH, str(today) + '-' + package_dict['id'] + '.html')

        if not os.path.isdir(self.DIFF_PATH):
            os.makedirs(self.DIFF_PATH)

        if os.path.isfile(new_metadata_path):
            if os.path.isfile(prev_metadata_path):
                with open(prev_metadata_path) as prev_metadata:
                    with open(new_metadata_path) as new_metadata:
                        if prev_metadata.read() != new_metadata.read():
                            with open(prev_metadata_path) as prev_metadata:
                                with open(new_metadata_path) as new_metadata:
                                    with open(diff_path, 'w') as diff:
                                        diff.write(
                                            "<!DOCTYPE html>\n<html>\n<body>\n<h2>Metadata diff for the dataset <a href=\""
                                            + self.INTERNAL_SITE_URL + "/dataset/" + package_dict['id'] + "\">"
                                            + package_dict['id'] + "</a></h2></body></html>\n"
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
                                        log.debug('Metadata diff generated for the dataset: ' + package_dict['id'])
                        else:
                            log.debug('No change in metadata for the dataset: ' + package_dict['id'])
                os.remove(prev_metadata_path)
                log.debug('Deleted previous day\'s metadata file.')
            else:
                log.debug('No earlier metadata JSON')

            os.rename(new_metadata_path, prev_metadata_path)

        else:
            log.debug(new_metadata_path + ' Metadata JSON missing for the dataset: ' + package_dict['id'])

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
            old_filename = r['name']
            r['name'] = munge_filename(r['name'])
            if r['resource_type'] == 'file':
                label = package_dict['datasetID'] + '/' + r['name']
                file_contents = ''
                with open(os.path.join(self.DATA_PATH, package_dict['datasetID'], self.META_DIR, old_filename)) as contents:
                    file_contents = contents.read()
                params = {
                    'filename-original': 'the original file name',
                    'uploaded-by': self.config['user']
                }
                r['url'] = self.CKAN_SITE_URL + '/storage/f/' + label
                self.get_ofs().put_stream(self.BUCKET, label, file_contents, params)

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
