# coding: utf-8

import datetime
import errno
import hashlib
import logging
import os
import re
import traceback
import uuid
from contextlib import contextmanager
from functools import cmp_to_key

import ckan.lib.navl.validators as validators
import ckan.plugins.toolkit as tk
import defusedxml.ElementTree as etree
from ckan import model
from ckan import plugins as p
from ckan.lib.helpers import json
from ckan.lib.munge import munge_tag, munge_title_to_name
from ckan.logic import NotFound, get_action
from ckan.model import Session
from werkzeug.datastructures import FileStorage as FlaskFileStorage

from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.stadtzhharvest.utils import (
    stadtzhharvest_create_new_context,
    stadtzhharvest_find_or_create_organization,
    stadtzhharvest_get_group_names,
)
from ckanext.stadtzhtheme.plugin import StadtzhThemePlugin

log = logging.getLogger(__name__)

FILE_NOT_FOUND_URL = "https://data.stadt-zuerich.ch/filenotfound"


class MetaXmlNotFoundError(Exception):
    pass


class MetaXmlInvalid(Exception):
    pass


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
    the_file = None
    while tries:
        try:
            the_file = open(path, mode)
        except IOError as e:
            error = e
            tries -= 1
            log.exception(
                "Error occured when opening %s: %r (tries left: %s)" % (path, e, tries)
            )
        else:
            break
    if not tries:
        if the_file and not the_file.closed:
            the_file.close()
        raise error
    yield the_file
    if close:
        the_file.close()


class StadtzhHarvester(HarvesterBase):
    """
    Harvester for the City of Zurich
    """

    def __init__(self, **kwargs):
        HarvesterBase.__init__(self, **kwargs)
        try:
            self.CKAN_SITE_URL = tk.config["ckan.site_url"]
        except KeyError as e:
            raise Exception("'%s' not found in config" % e.message)

    def info(self):
        return {
            "name": "stadtzh_harvester",
            "title": "Harvester for the City of Zurich",
            "description": "Harvester for the DWH and GEO dropzones of the City of Zurich",
        }

    def validate_config(self, config_str):
        config_obj = json.loads(config_str)
        self._validate_string_config(config_obj, "data_path", required=True)
        self._validate_string_config(config_obj, "metafile_dir")
        self._validate_string_config(config_obj, "dataset_prefix")
        self._validate_boolean_config(config_obj, "update_datasets")
        self._validate_boolean_config(config_obj, "update_date_last_modified")
        self._validate_boolean_config(
            config_obj, "delete_missing_datasets", required=False
        )

        return config_str

    def _validate_string_config(self, source, field, required=False):
        if field in source:
            value = source[field]
            if not isinstance(value, str):
                raise ValueError("%s must be a string" % field)
        elif required:
            raise ValueError("%s is required" % field)

    def _validate_boolean_config(self, source, field, required=True):
        if field in source:
            value = source[field]
            if not isinstance(value, bool):
                raise ValueError("%s must be a boolean" % field)
        elif required:
            raise ValueError("%s is required" % field)

    def _set_config(self, config_str):
        self.config = json.loads(config_str)

        if "metafile_dir" not in self.config:
            self.config["metafile_dir"] = ""
        if "update_datasets" not in self.config:
            self.config["update_datasets"] = False
        if "update_date_last_modified" not in self.config:
            self.config["update_date_last_modified"] = False
        if "dataset_prefix" not in self.config:
            self.config["dataset_prefix"] = ""
        if "delete_missing_datasets" not in self.config:
            self.config["delete_missing_datasets"] = False

        log.debug("Using config: %r" % self.config)

    def gather_stage(self, harvest_job):
        log.debug("In StadtzhHarvester gather_stage")
        self._set_config(harvest_job.source.config)

        # generated ids of the harvest objects
        ids = []
        # cleaned dataset names used as ids for the datasets
        gathered_dataset_ids = []
        try:
            # list directories in dropzone folder
            datasets = self._remove_hidden_files(os.listdir(self.config["data_path"]))
            log.debug("Directories in %s: %s" % (self.config["data_path"], datasets))

            # foreach -> meta.xml -> create entry
            for dataset in datasets:
                # use dataset_prefix to make dataset names unique
                dataset_name = "%s%s" % (self.config["dataset_prefix"], dataset)
                dataset_id = self._validate_package_id(dataset_name)
                log.debug("Gather %s" % dataset_id)
                if dataset_id:
                    gathered_dataset_ids.append(dataset_id)
                    meta_xml_path = os.path.join(
                        self.config["data_path"],
                        dataset,
                        self.config["metafile_dir"],
                        "meta.xml",
                    )
                    try:
                        metadata = self._load_metadata_from_path(
                            meta_xml_path, dataset_id, dataset
                        )
                    except Exception as e:
                        log.exception(e)
                        self._save_gather_error(
                            "Could not parse metadata in %s: %s / %s"
                            % (meta_xml_path, str(e), traceback.format_exc()),
                            harvest_job,
                        )
                        continue

                    id = self._save_harvest_object(metadata, harvest_job)
                    ids.append(id)
            if self.config["delete_missing_datasets"]:
                delete_ids = self._check_for_deleted_datasets(
                    harvest_job, gathered_dataset_ids
                )
                ids.extend(delete_ids)

            return ids
        except Exception as e:
            log.exception(e)
            self._save_gather_error(
                "Unable to get content from folder: %s: %s / %s"
                % (self.config["data_path"], str(e), traceback.format_exc()),
                harvest_job,
            )
            return []

    def _load_metadata_from_path(self, meta_xml_path, dataset_id, dataset):
        if not os.path.exists(meta_xml_path):
            raise MetaXmlNotFoundError(
                "meta.xml not found for dataset %s (path: %s)"
                % (dataset_id, meta_xml_path)
            )

        with retry_open_file(meta_xml_path, "r") as meta_xml:
            meta_xml = etree.parse(meta_xml)
            dataset_node = meta_xml.find("datensatz")
            resources_node = dataset_node.find("ressourcen")

        metadata = self._dropzone_get_metadata(dataset_id, dataset, dataset_node)

        # add resource metadata
        metadata["resource_metadata"] = self._get_resources_metadata(resources_node)

        return metadata

    def fetch_stage(self, harvest_object):
        log.debug("In StadtzhHarvester fetch_stage")
        # Nothing to do here
        return True

    def import_stage(self, harvest_object):
        log.debug("In StadtzhHarvester import_stage")
        self._set_config(harvest_object.job.source.config)

        if not harvest_object:
            log.error("No harvest object received")
            self._save_object_error("No harvest object received", harvest_object)
            return False

        try:
            return self._import_package(harvest_object)
        except Exception as e:
            log.exception(e)
            self._save_object_error(
                (
                    "Unable to get content for package: %s: %r / %s"
                    % (harvest_object.guid, e, traceback.format_exc())
                ),
                harvest_object,
            )
            return False
        finally:
            Session.commit()

    def _import_package(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        package_dict["id"] = harvest_object.guid
        package_dict["name"] = munge_title_to_name(package_dict["datasetID"])
        context = stadtzhharvest_create_new_context()

        # check if dataset must be deleted
        import_action = package_dict.pop("import_action", "update")
        if import_action == "delete":
            harvest_object.current = False
            return self._delete_dataset(package_dict)

        # check if package already exists and
        existing_package = self._get_existing_package(package_dict)

        # get metadata for resources
        resource_metadata = package_dict.pop("resource_metadata", {})
        new_resources = self._generate_resources_from_folder(
            package_dict["datasetFolder"]
        )
        for resource in new_resources:
            if resource["name"] in resource_metadata:
                resource.update(resource_metadata[resource["name"]])

        # set the actions to do with the resources after the package is
        # updated or created
        actions, resources_changed = self._resources_actions(
            existing_package, new_resources
        )

        if existing_package and "resources" in existing_package:
            package_dict["resources"] = existing_package["resources"]
        stadtzhharvest_find_or_create_organization(package_dict)

        # import the package if it does not yet exists => it's a new package
        # or if this harvester is allowed to update packages
        if not existing_package:
            dataset_id = self._create_package(package_dict, harvest_object)
            if not dataset_id:
                # No need to log an error here
                # as it was logged in _create_package
                return False
        else:
            # Don't change the dataset name even if the title has
            package_dict["name"] = existing_package["name"]
            package_dict["id"] = existing_package["id"]
            dataset_id = self._update_package(package_dict, harvest_object)

        # set the date_last_modified if any resource changed
        if self.config["update_date_last_modified"] and resources_changed:
            theme_plugin = StadtzhThemePlugin()
            package_schema = theme_plugin.update_package_schema()
            schema_context = stadtzhharvest_create_new_context()
            schema_context["ignore_auth"] = True
            schema_context["schema"] = package_schema
            today = datetime.datetime.now().strftime("%d.%m.%Y")
            try:
                get_action("package_patch")(
                    schema_context, {"id": dataset_id, "dateLastUpdated": today}
                )
            except p.toolkit.ValidationError as e:
                self._save_object_error(
                    "Update validation Error: %s" % str(e.error_summary),
                    harvest_object,
                    "Import",
                )
                return False
            log.info("Updated dateLastUpdated to %s", today)
        else:
            log.info(
                "dateLastUpdated *not* updated because "
                "update_date_last_modified config is set to `false`"
            )

        # handle all resources (create, update, delete)
        resource_ids = self._import_resources(actions, package_dict, harvest_object)
        ordered_resource_ids = _keep_order_of_existing_resources(
            package_dict, resource_ids
        )
        reorder = {"id": str(package_dict["id"]), "order": ordered_resource_ids}
        tk.get_action("package_resource_reorder")(context.copy(), data_dict=reorder)
        Session.commit()
        return True

    def _delete_dataset(self, package_dict):
        context = stadtzhharvest_create_new_context()
        get_action("dataset_purge")(context.copy(), package_dict)
        return True

    def _get_existing_package(self, package_dict):
        context = stadtzhharvest_create_new_context()
        try:
            existing_package = get_action("package_show")(
                context, {"id": package_dict["id"]}
            )
        except NotFound:
            existing_package = None
            log.debug("Could not find pkg %s" % package_dict["name"])
        return existing_package

    def _get_existing_packages_names(self, harvest_job):
        context = stadtzhharvest_create_new_context()
        n = 500
        page = 1
        existing_packages_names = []
        while True:
            search_params = {
                "fq": 'harvest_source_id:"{0}"'.format(harvest_job.source_id),
                "rows": n,
                "start": n * (page - 1),
            }
            try:
                existing_packages = get_action("package_search")(context, search_params)
                if len(existing_packages["results"]):
                    existing_packages_names.extend(
                        [pkg["name"] for pkg in existing_packages["results"]]
                    )
                    page = page + 1
                else:
                    break
            except NotFound:
                if page == 1:
                    log.debug(
                        "Could not find pkges for source %s" % harvest_job.source_id
                    )
        log.info(
            "Found %d number of packages for source %s"
            % (len(existing_packages_names), harvest_job.source_id)
        )
        return existing_packages_names

    def _resources_actions(self, existing_package, new_resources):
        resources_changed = False
        actions = []

        if not existing_package:
            resources_changed = True
            for r in new_resources:
                actions.append(
                    {"action": "create", "new_resource": r, "res_name": r["name"]}
                )
        else:
            old_resources = existing_package["resources"]
            for r in new_resources:
                action = {
                    "action": "create",
                    "new_resource": r,
                    "old_resource": None,
                    "res_name": r["name"],
                }
                for old in old_resources:
                    if old["name"] == r["name"]:
                        action["action"] = "update"
                        action["old_resource"] = old

                        # check if the resource changed
                        if (
                            r.get("zh_hash")
                            and old.get("zh_hash")
                            and r["zh_hash"] != old["zh_hash"]
                        ):
                            resources_changed = True
                        break
                actions.append(action)

            for old in old_resources:
                if not filter(
                    lambda action: action["res_name"] == old["name"], actions
                ):
                    actions.append(
                        {
                            "action": "delete",
                            "old_resource": old,
                            "res_name": old["name"],
                        }
                    )
        return (actions, resources_changed)

    def _import_resources(self, actions, package_dict, harvest_object):
        actions.sort(key=_sort_new_resources_by_name)
        resource_ids = []
        context = stadtzhharvest_create_new_context()
        for action in actions:
            res_name = action["res_name"]
            try:
                resource_id = None
                log.debug("Resource %s, action: %s" % (res_name, action))
                if action["action"] == "create":
                    resource = dict(action["new_resource"])
                    resource["package_id"] = package_dict["id"]
                    resource_id = get_action("resource_create")(
                        context.copy(), resource
                    )["id"]
                    resource_ids.append(resource_id)
                    log.debug("Dataset resource `%s` has been created" % resource_id)

                elif action["action"] == "update":
                    resource = dict(action["old_resource"])
                    resource["package_id"] = package_dict["id"]

                    if "upload" in action["new_resource"]:
                        # if the resource is an upload, replace the file
                        resource["upload"] = action["new_resource"]["upload"]
                    elif action["new_resource"]["resource_type"] == "api":
                        # for APIs, update the URL
                        resource["url"] = action["new_resource"]["url"]

                    # update fields from new resource
                    resource["description"] = action["new_resource"].get("description")
                    resource["format"] = action["new_resource"].get("format")
                    resource["zh_hash"] = action["new_resource"].get("zh_hash")

                    log.debug("Trying to update resource: %s" % resource)
                    resource_id = get_action("resource_update")(
                        context.copy(), resource
                    )["id"]
                    resource_ids.append(resource_id)
                    log.debug("Dataset resource `%s` has been updated" % resource_id)

                elif action["action"] == "delete":
                    replace_upload = get_action("resource_update")(
                        context.copy(),
                        {
                            "id": action["old_resource"]["id"],
                            "url": FILE_NOT_FOUND_URL,
                            "clear_upload": "true",
                        },
                    )
                    log.debug("Dataset resource has been cleared: %s" % replace_upload)

                    result = get_action("resource_delete")(
                        context.copy(), {"id": action["old_resource"]["id"]}
                    )
                    log.debug("Dataset resource has been deleted: %s" % result)

                else:
                    raise ValueError("Unknown action, we should never reach this point")

            except Exception as e:
                self._save_object_error(
                    "Error while handling action %s for resource %s in pkg %s: %r %s"
                    % (
                        action,
                        res_name,
                        package_dict["name"],
                        e,
                        traceback.format_exc(),
                    ),
                    harvest_object,
                    "Import",
                )
                continue
        return resource_ids

    def _create_package(self, dataset, harvest_object):
        theme_plugin = StadtzhThemePlugin()
        package_schema = theme_plugin.create_package_schema()

        # We need to explicitly provide a package ID
        dataset["id"] = str(uuid.uuid4())
        package_schema["id"] = [validators.unicode_safe]

        # get the site user
        site_user = tk.get_action("get_site_user")(
            {"model": model, "ignore_auth": True}, {}
        )
        context = {
            "user": site_user["name"],
            "return_id_only": True,
            "ignore_auth": True,
            "schema": package_schema,
        }

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        # Save reference to the package on the object
        harvest_object.package_id = dataset["id"]
        harvest_object.add()

        # Defer constraints and flush so the dataset can be indexed with
        # the harvest object id (on the after_show hook from the harvester
        # plugin)
        model.Session.execute("SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED")
        model.Session.flush()

        try:
            p.toolkit.get_action("package_create")(context, dataset)
        except p.toolkit.ValidationError as e:
            self._save_object_error(
                "Create validation Error: %s" % str(e.error_summary),
                harvest_object,
                "Import",
            )
            return False

        log.info("Created dataset %s", dataset["name"])

        model.Session.commit()

        return dataset["id"]

    def _update_package(self, dataset, harvest_object):
        # Get the last harvested object (if any)
        previous_object = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.guid == harvest_object.guid)
            .filter(HarvestObject.current == True)
            .first()
        )

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        # Save reference to the package on the object
        harvest_object.package_id = dataset["id"]
        harvest_object.add()

        # Defer constraints and flush so the dataset can be indexed with
        # the harvest object id (on the after_show hook from the harvester
        # plugin)
        model.Session.execute("SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED")
        model.Session.flush()

        # only update pkg if this harvester allows it
        if self.config["update_datasets"]:
            theme_plugin = StadtzhThemePlugin()

            # get site user
            site_user = tk.get_action("get_site_user")(
                {"model": model, "ignore_auth": True}, {}
            )
            context = {
                "user": site_user["name"],
                "return_id_only": True,
                "ignore_auth": True,
                "schema": theme_plugin.update_package_schema(),
            }
            try:
                get_action("package_update")(context, dataset)
            except p.toolkit.ValidationError as e:
                self._save_object_error(
                    "Update validation Error: %s" % str(e.error_summary),
                    harvest_object,
                    "Import",
                )
                return False
            log.info("Updated dataset %s", dataset["name"])
        else:
            log.info(
                "Dataset %s *not* updated because update_datasets"
                "config is set to `false`" % dataset["name"]
            )

        model.Session.commit()
        return dataset["id"]

    def _save_harvest_object(self, metadata, harvest_job):
        """
        Save the harvest object with the given metadata dict and harvest_job
        """

        obj = HarvestObject(
            guid=metadata["datasetID"], job=harvest_job, content=json.dumps(metadata)
        )
        obj.save()
        log.debug("adding " + metadata["datasetID"] + " to the queue")

        return obj.id

    def _dropzone_get_groups(self, dataset_node):
        """
        Get the groups from the node, normalize them and get the names.
        """
        categories = self._get(dataset_node, "kategorie")
        if categories:
            group_titles = categories.split(", ")
            groups = []
            for title in group_titles:
                name = munge_title_to_name(title)
                groups.append((name, title))
            return stadtzhharvest_get_group_names(groups)
        else:
            return []

    def _dropzone_get_metadata(self, dataset_id, dataset_folder, dataset_node):
        """
        For the given dataset node return the metadata dict.
        """

        return {
            "datasetID": dataset_id,
            "datasetFolder": dataset_folder,
            "title": dataset_node.find("titel").text,
            "url": self._get(dataset_node, "lieferant"),
            "notes": dataset_node.find("beschreibung").text,
            "author": dataset_node.find("quelle").text,
            "maintainer": "Open Data Zürich",
            "maintainer_email": "opendata@zuerich.ch",
            "license_id": self._get(dataset_node, "lizenz", default="cc-zero"),
            "tags": self._generate_tags(dataset_node),
            "groups": self._dropzone_get_groups(dataset_node),
            "spatialRelationship": self._get(dataset_node, "raeumliche_beziehung"),
            "dateFirstPublished": self._get(
                dataset_node, "erstmalige_veroeffentlichung"
            ),
            "dateLastUpdated": self._get(dataset_node, "aktualisierungsdatum"),
            "updateInterval": self._get_update_interval(dataset_node),
            "dataType": self._get_data_type(dataset_node),
            "legalInformation": self._get(dataset_node, "rechtsgrundlage"),
            "version": self._get(dataset_node, "aktuelle_version"),
            "timeRange": self._get(dataset_node, "zeitraum"),
            "sszBemerkungen": self._convert_comments(dataset_node),
            "sszFields": self._json_encode_attributes(
                self._get_attributes(dataset_node)
            ),
            "dataQuality": self._get(dataset_node, "datenqualitaet"),
        }

    def _get_update_interval(self, dataset_node):
        interval = (
            self._get(dataset_node, "aktualisierungsintervall")
            .replace("ä", "ae")
            .replace("ö", "oe")
            .replace("ü", "ue")
        )
        if not interval:
            return "   "
        return interval

    def _get_data_type(self, dataset_node):
        data_type = self._get(dataset_node, "datentyp")
        if not data_type:
            return "   "
        return data_type

    def _remove_hidden_files(self, file_list):
        """
        Removes dotfiles from a list of files
        """
        cleaned_file_list = []
        for file in file_list:
            if not file.startswith("."):
                cleaned_file_list.append(file)
        return cleaned_file_list

    def _generate_tags(self, dataset_node):
        """
        Given a dataset node it extracts the tags and returns them in an array
        """
        tags = []
        tags_node = dataset_node.find("schlagworte")
        if tags_node is not None and tags_node.text:
            for tag in tags_node.text.split(", "):
                tags.append({"name": munge_tag(tag)})
        log.debug("Added tags: %s" % str(tags))
        return tags

    def _sort_resource(self, x, y):
        order = {
            "csv": 0,
            "shp": 1,
            "wms": 2,
            "wmts": 3,
            "wfs": 4,
            "json": 5,
            "kmz": 6,
            "kml": 7,
            "pkgk": 8,
            "gpkg": 9,
            "swp": 10,
            "zip": 11,
            "txt": 12,
            "xlsx": 13,
            "pdf": 14,
        }

        x_format = x["format"].lower()
        y_format = y["format"].lower()
        if x_format not in order:
            return 1
        if y_format not in order:
            return -1
        return order[x_format] - order[y_format]

    def _get_resources_metadata(self, resources_node):
        resources = {}
        if resources_node:
            for resource in resources_node:
                filename = resource.get("dateiname")
                if not filename:
                    raise MetaXmlInvalid("Resources must have an attribute 'dateiname'")
                resources[filename] = {
                    "description": self._get(resource, "beschreibung"),
                }
        return resources

    def _generate_resources_from_folder(self, dataset):
        """
        Given a dataset folder, it'll return a list of resource metadata
        """
        resources = []
        file_list = [
            f
            for f in os.listdir(
                os.path.join(
                    self.config["data_path"], dataset, self.config["metafile_dir"]
                )
            )
            if os.path.isfile(
                os.path.join(
                    self.config["data_path"], dataset, self.config["metafile_dir"], f
                )
            )
        ]
        resource_files = self._remove_hidden_files(file_list)
        log.debug(resource_files)

        # for resource_file in resource_files:
        for resource_file in (x for x in resource_files if x != "meta.xml"):
            resource_path = os.path.join(
                self.config["data_path"],
                dataset,
                self.config["metafile_dir"],
                resource_file,
            )
            if resource_file == "link.xml":
                with retry_open_file(resource_path, "r") as links_xml:
                    links = etree.parse(links_xml).findall("link")

                    for link in links:
                        url = self._get(link, "url")
                        if url:
                            # generate hash for URL
                            md5 = hashlib.md5()
                            md5.update(url.encode("utf-8"))
                            resources.append(
                                {
                                    "url": url,
                                    "zh_hash": md5.hexdigest(),
                                    "name": self._get(link, "lable"),
                                    "description": self._get(link, "description"),
                                    "format": self._get(link, "type"),
                                    "resource_type": "api",
                                }
                            )
            else:
                resource_file = self._validate_filename(resource_file)
                if resource_file:
                    resource_dict = {
                        "name": resource_file,
                        "url": "",
                        "description": "",
                        "url_type": "upload",
                        "format": resource_file.split(".")[-1],
                        "resource_type": "file",
                    }

                    # calculate the hash of this file
                    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
                    md5 = hashlib.md5()
                    with retry_open_file(resource_path, "rb") as f:
                        while True:
                            data = f.read(BUF_SIZE)
                            if not data:
                                break
                            md5.update(data)
                        resource_dict["zh_hash"] = md5.hexdigest()

                    # add file to FieldStorage
                    with retry_open_file(resource_path, "rb", close=False) as f:
                        field_storage = FlaskFileStorage(f, f.name)
                        resource_dict["upload"] = field_storage

                    resources.append(resource_dict)

        sorted_resources = sorted(resources, key=cmp_to_key(self._sort_resource))
        return sorted_resources

    def _node_exists_and_is_nonempty(self, dataset_node, element_name):
        element = dataset_node.find(element_name)
        if element is None or element.text is None:
            return None
        return element.text

    def _get(self, node, name, default=""):
        element = self._node_exists_and_is_nonempty(node, name)
        if element:
            return element
        else:
            return default

    def _convert_comments(self, node):
        comments = node.find("bemerkungen")
        if comments is not None:
            log.debug(comments.tag + " " + str(comments.attrib))
            markdown = ""
            for comment in comments:
                if self._get(comment, "titel"):
                    markdown += "**" + self._get(comment, "titel") + "**\n\n"
                if self._get(comment, "text"):
                    markdown += self._get(comment, "text") + "\n\n"
                link = comment.find("link")
                if link is not None:
                    label = self._get(link, "label")
                    url = self._get(link, "url")
                    markdown += "[" + label + "](" + url + ")\n\n"
            return markdown

    def _json_encode_attributes(self, properties):
        attributes = []
        for key, value in properties:
            if value:
                attributes.append((key, value))

        return json.dumps(attributes)

    def _get_attributes(self, node):
        attribut_list = node.find("attributliste")
        attributes = []
        for attribut in attribut_list:
            tech_name = attribut.get("technischerfeldname")
            speak_name = attribut.find("sprechenderfeldname").text

            if tech_name:
                attribute_name = "%s (technisch: %s)" % (speak_name, tech_name)
            else:
                attribute_name = speak_name

            attributes.append((attribute_name, attribut.find("feldbeschreibung").text))
        return attributes

    def _get_immediate_subdirectories(self, directory):
        try:
            return [
                name
                for name in os.listdir(directory)
                if os.path.isdir(os.path.join(directory, name))
            ]
        except OSError as e:
            if e.errno == errno.ENOENT:
                # directory does not exist
                return []
            raise

    def _diff_path(self, package_id):
        today = datetime.date.today()
        if package_id:
            return os.path.join(self.DIFF_PATH, "%s-%s.html" % (str(today), package_id))

    def _check_for_deleted_datasets(self, harvest_job, gathered_dataset_names):
        existing_packages_names = self._get_existing_packages_names(harvest_job)
        delete_names = list(set(existing_packages_names) - set(gathered_dataset_names))
        # gather delete harvest ids
        delete_ids = []

        for package_name in delete_names:
            log.debug("Dataset `%s` has been deleted" % package_name)

            if self.config["delete_missing_datasets"]:
                log.info("Add `%s` for deletion", package_name)
                id = self._save_harvest_object(
                    {"datasetID": package_name, "import_action": "delete"}, harvest_job
                )
                delete_ids.append(id)
        return delete_ids

    def _find_or_create_organization(self, package_dict, context):
        # Find or create the organization the dataset should get assigned to.
        try:
            data_dict = {
                "id": munge_title_to_name(self.ORGANIZATION["de"]),
            }
            package_dict["owner_org"] = get_action("organization_show")(
                context.copy(), data_dict
            )["id"]
        except Exception:
            data_dict = {
                "permission": "edit_group",
                "id": munge_title_to_name(self.ORGANIZATION["de"]),
                "name": munge_title_to_name(self.ORGANIZATION["de"]),
                "title": self.ORGANIZATION["de"],
            }
            organization = get_action("organization_create")(context.copy(), data_dict)
            package_dict["owner_org"] = organization["id"]

    def _validate_package_id(self, package_id):
        # Validate that they do not contain any HTML tags.
        match = re.search("[<>]+", package_id)
        if match:
            log.debug("Package id %s contains disallowed characters" % package_id)
            return False
        else:
            return munge_title_to_name(package_id).strip("-")

    def _validate_filename(self, filename):
        # Validate that they do not contain any HTML tags.
        match = re.search("[<>]+", filename)
        if len(filename) == 0:
            log.debug("Filename is empty.")
            return False
        if match:
            log.debug(
                "Filename %s not added as it contains disallowed characters" % filename
            )
            return False
        else:
            return filename


def _keep_order_of_existing_resources(package_dict, resource_ids):
    """keep order of existing resources and put new resources
    at the end of the list"""
    existing_resource_ids = []
    if package_dict.get("resources"):
        existing_resource_ids = [
            resource["id"]
            for resource in package_dict["resources"]
            if resource["id"] in resource_ids
        ]
    new_resource_ids = [id for id in resource_ids if id not in existing_resource_ids]
    return existing_resource_ids + new_resource_ids


def _sort_new_resources_by_name(action):
    """order new resources by their name"""
    if action.get("new_resource"):
        return action["new_resource"].get("name").lower()
    else:
        return action["old_resource"].get("id")
