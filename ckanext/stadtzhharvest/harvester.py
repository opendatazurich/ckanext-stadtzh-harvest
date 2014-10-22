# coding: utf-8

import os
from lxml import etree
from ofs import get_impl
from pylons import config
from ckan import model
from ckan.model import Session
from ckan.logic import action
from ckan.lib.helpers import json
from ckanext.harvest.harvesters import HarvesterBase

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
    CKAN_SITE_URL = 'https://ogd-integ.global.szh.loc'

    config = {
        'user': u'harvest'
    }

    DIFF_PATH = config.get('metadata.diffpath', '/usr/lib/ckan/diffs')
    INTERNAL_SITE_URL = config.get('ckan.site_url_internal', 'https://ogd-integ.global.szh.loc')

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
        resource_files = self._remove_hidden_files((f for f in os.listdir(os.path.join(self.DROPZONE_PATH, dataset))
            if os.path.isfile(os.path.join(self.DROPZONE_PATH, dataset, f))))
        log.debug(resource_files)

        # for resource_file in resource_files:
        for resource_file in (x for x in resource_files if x != 'meta.xml'):
            if resource_file == u'link.xml':
                with open(os.path.join(self.DROPZONE_PATH, dataset, resource_file), 'r') as links_xml:
                    parser = etree.XMLParser(encoding='utf-8')
                    links = etree.fromstring(links_xml.read(), parser=parser).findall('link')
                    for link in links:
                        if link.find('url').text != "" and link.find('url').text != None:
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
                        'title': self._get(item, 'beschreibung'),
                        'type': related_type,
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
                log.debug('Creating related %s' % entry)
                action.create.related_create(context, entry)

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
