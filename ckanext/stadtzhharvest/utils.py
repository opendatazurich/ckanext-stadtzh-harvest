# coding: utf-8

from ckan.lib.munge import munge_title_to_name
from ckan.logic import get_action
from ckan import model
from ckan.model import Session

ORGANIZATION = {
    'de': u'Stadt Zürich',
    'fr': u'fr_Stadt Zürich',
    'it': u'it_Stadt Zürich',
    'en': u'en_Stadt Zürich',
}


def stadtzhharvest_find_or_create_organization(package_dict):
    # Find or create the organization the dataset should get assigned to.
    context = stadtzhharvest_create_new_context()
    try:
        data_dict = {
            'id': munge_title_to_name(ORGANIZATION['de']),
        }
        package_dict['owner_org'] = get_action('organization_show')(
            context.copy(),
            data_dict
        )['id']
        return package_dict
    except Exception:
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
        return package_dict


def stadtzhharvest_create_new_context():
    # get the site user
    site_user = get_action('get_site_user')(
                          {'model': model, 'ignore_auth': True}, {})
    context = {
        'model': model,
        'session': Session,
        'user': site_user['name'],
    }
    return context
