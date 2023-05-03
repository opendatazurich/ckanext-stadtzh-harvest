#!/bin/bash

# Install requirements and ckanext
pip install -r /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/requirements.txt
pip install -r /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/dev-requirements.txt
pip install -e /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/
pip install -U requests[security]

# Install ckanext dependencies
pip install -e git+https://github.com/ckan/ckanext-xloader.git#egg=ckanext-xloader
pip install -r https://raw.githubusercontent.com/ckan/ckanext-xloader/master/requirements.txt
pip install -e git+https://github.com/opendatazurich/ckanext-stadtzh-theme.git@dockerizing#egg=ckanext-stadtzh-theme
pip install -r https://raw.githubusercontent.com/opendatazurich/ckanext-stadtzh-theme/dockerizing/pip-requirements.txt

if [ "$1" = "2.9-py2" ]; then
  # Install most recent versions that still support Py2
  pip install -e git+https://github.com/ckan/ckanext-dcat.git@0c26bed5b7a3a7fca8e7b78e338aace096e0ebf6#egg=ckanext-dcat
  pip install -r https://raw.githubusercontent.com/ckan/ckanext-dcat/0c26bed5b7a3a7fca8e7b78e338aace096e0ebf6/requirements-py2.txt
  pip install -e git+https://github.com/ckan/ckanext-harvest.git@a628782a984a7ee9ac2c51236d3fad4b4e9c7fbb#egg=ckanext-harvest
  pip install -r https://raw.githubusercontent.com/ckan/ckanext-harvest/a628782a984a7ee9ac2c51236d3fad4b4e9c7fbb/requirements.txt
else
  pip install -e git+https://github.com/ckan/ckanext-dcat.git#egg=ckanext-dcat
  pip install -r https://raw.githubusercontent.com/ckan/ckanext-dcat/master/requirements.txt
  pip install -e git+https://github.com/ckan/ckanext-harvest.git#egg=ckanext-harvest
  pip install -r https://raw.githubusercontent.com/ckan/ckanext-harvest/master/requirements.txt
fi

# Replace default path to CKAN core config file with the one on the container
sed -i -e 's/use = config:.*/use = config:\/srv\/app\/src\/ckan\/test-core.ini/' /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/test.ini

# Init db and enable required plugins
ckan config-tool /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/test.ini "ckan.plugins = "
ckan -c /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/test.ini db init
ckan config-tool /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/test.ini "ckan.plugins = harvest stadtzh_harvester stadtzhtheme"
