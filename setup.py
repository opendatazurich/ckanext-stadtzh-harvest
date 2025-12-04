from setuptools import find_packages, setup

version = "0.0"

setup(
    name="ckanext-stadtzh-harvest",
    version=version,
    description="CKAN Harvester for the City of Zurich",
    long_description="""\
    """,
    classifiers=[],
    keywords="",
    author="Liip AG",
    author_email="ogd@liip.ch",
    url="http://www.liip.ch",
    license="AGPL-3.0-or-later",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    namespace_packages=["ckanext"],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
    ],
    entry_points="""
    [ckan.plugins]
    stadtzh_harvester=ckanext.stadtzhharvest.harvester:StadtzhHarvester
    """,
)
