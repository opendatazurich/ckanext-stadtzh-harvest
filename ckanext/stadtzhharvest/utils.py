# coding: utf-8

import logging
import traceback

import ckan.plugins.toolkit as tk
from ckan import model
from ckan.lib.munge import munge_title_to_name
from ckan.logic import get_action
from ckan.model import Session

log = logging.getLogger(__name__)

ORGANIZATION = {
    "de": "Stadt Zürich",
    "fr": "fr_Stadt Zürich",
    "it": "it_Stadt Zürich",
    "en": "en_Stadt Zürich",
}


def stadtzhharvest_find_or_create_organization(package_dict):
    # Find or create the organization the dataset should get assigned to.
    context = stadtzhharvest_create_new_context()
    try:
        data_dict = {
            "id": munge_title_to_name(ORGANIZATION["de"]),
        }
        package_dict["owner_org"] = get_action("organization_show")(
            context.copy(), data_dict
        )["id"]
        return package_dict
    except Exception:
        data_dict = {
            "permission": "edit_group",
            "id": munge_title_to_name(ORGANIZATION["de"]),
            "name": munge_title_to_name(ORGANIZATION["de"]),
            "title": ORGANIZATION["de"],
        }
        organization = get_action("organization_create")(context.copy(), data_dict)
        package_dict["owner_org"] = organization["id"]
        return package_dict


def stadtzhharvest_create_new_context():
    # get the site user
    site_user = get_action("get_site_user")({"model": model, "ignore_auth": True}, {})
    context = {
        "model": model,
        "session": Session,
        "user": site_user["name"],
    }
    return context


def stadtzhharvest_get_group_names(group_list):
    """Return the group names for the given groups.
    The list should contain group tuples: (name, title)
    If a group does not exist in CKAN, create it.
    """
    # get site user
    site_user = tk.get_action("get_site_user")(
        {"model": model, "ignore_auth": True}, {}
    )

    context = {
        "model": model,
        "session": Session,
        "ignore_auth": True,
        "user": site_user["name"],
    }
    groups = []
    for name, title in group_list:
        data_dict = {"id": name}
        try:
            group_name = get_action("group_show")(context.copy(), data_dict)["name"]
            groups.append({"name": group_name})
            log.debug("Added group %s" % name)
        except Exception:
            data_dict["name"] = name
            data_dict["title"] = title
            data_dict["image_url"] = "%s/kategorien/%s.png" % (
                tk.config["ckan.site_url"],
                name,
            )
            log.debug(
                "Couldn't get group id. "
                "Creating the group `%s` with data_dict: %s" % (name, data_dict)
            )
            try:
                group = get_action("group_create")(context.copy(), data_dict)
                log.debug("Created group %s" % group)
                groups.append({"name": group["name"]})
            except Exception:
                log.debug("Couldn't create group: %s" % (traceback.format_exc()))
                raise

    return groups
