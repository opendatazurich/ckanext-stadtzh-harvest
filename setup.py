from setuptools import setup, find_packages

version = '0.0'

setup(
    name='ckanext-stadtzh-harvest',
    version=version,
    description="CKAN Harvester Base for the City of Zurich",
    long_description="""\
    """,
    classifiers=[],
    keywords='',
    author='Liip AG',
    author_email='ogd@liip.ch',
    url='http://www.liip.ch',
    license='GPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.stadtzhharvest'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
        'lxml==2.2.4',
    ],
    entry_points=
    """
    """,
)
