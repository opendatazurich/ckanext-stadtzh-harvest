# coding: utf-8

from ckan.lib.munge import munge_title_to_name
from ckan.logic import get_action, NotFound

ORGANIZATION = {
    'de': u'Stadt Zürich',
    'fr': u'fr_Stadt Zürich',
    'it': u'it_Stadt Zürich',
    'en': u'en_Stadt Zürich',
}


def stadtzhharvest_find_or_create_organization(package_dict, context):
    # Find or create the organization the dataset should get assigned to.
    try:
        data_dict = {
            'id': munge_title_to_name(ORGANIZATION['de']),
        }
        package_dict['owner_org'] = get_action('organization_show')(
            context.copy(),
            data_dict
        )['id']
    except:
        data_dict = {
            'permission': 'edit_group',
            'id': munge_title_to_name(ORGANIZATION['de']),
            'name': munge_title_to_name(ORGANIZATION['de']),
            'title': ORGANIZATION['de']
        }
        organization = get_action('organization_create')(
            context.copy(),
            data_dict
        )
        package_dict['owner_org'] = organization['id']
