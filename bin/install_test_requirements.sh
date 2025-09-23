#!/bin/bash

# Install requirements and ckanext
pip install -r /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/requirements.txt
pip install -r /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/dev-requirements.txt
pip install -e /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/
pip install -U requests[security]

# Install ckanext dependencies
pip install -e git+https://github.com/ckan/ckanext-xloader.git#egg=ckanext-xloader
pip install -r https://raw.githubusercontent.com/ckan/ckanext-xloader/master/requirements.txt
pip install -e git+https://github.com/opendatazurich/ckanext-stadtzh-theme.git@main#egg=ckanext-stadtzh-theme
pip install -r https://raw.githubusercontent.com/opendatazurich/ckanext-stadtzh-theme/main/pip-requirements.txt
pip install -e git+https://github.com/ckan/ckanext-dcat.git#egg=ckanext-dcat
pip install -r https://raw.githubusercontent.com/ckan/ckanext-dcat/master/requirements.txt
pip install -e git+https://github.com/ckan/ckanext-harvest.git#egg=ckanext-harvest
pip install -r https://raw.githubusercontent.com/ckan/ckanext-harvest/master/requirements.txt

# Replace default path to CKAN core config file with the one on the container
sed -i -e 's/use = config:.*/use = config:\/srv\/app\/src\/ckan\/test-core.ini/' /__w/ckanext-stadtzh-harvest/ckanext-stadtzh-harvest/test.ini

# Init db and apply any pending migrations
ckan -c /__w/ckanext-switzerland/ckanext-switzerland/test.ini db init
ckan -c /__w/ckanext-switzerland/ckanext-switzerland/test.ini db pending-migrations --apply
