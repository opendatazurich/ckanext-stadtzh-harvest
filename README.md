ckanext-stadtzh-harvest
=======================

Harvester for the City of Zurich.
The data is loaded from so called "dropzones", which are mounted network drives.
Each folder in a dropzone is considered a dataset.

## Content

* [Configuration](#configuration)
* [Metadata](#metadata)
	* [meta.xml](#metaxml)
	* [link.xml](#linkxml)


## Configuration

The harvest configuration must be provided as a valid JSON string.

Example:

```json
{
    "data_path": "/home/liip/dropzones/GEO",
    "metadata_dir": "geo-metadata",
    "metafile_dir": "DEFAULT",
    "update_datasets": false,
    "update_date_last_modified": true
}
```

### `data_path`

The path to the dropzone

### `metadata_dir`

The path where the diff files are stored, this is used to compare the metadata from the previous days harvest job.

### `metafile_dir`

The name of the directory where the `meta.xml` is located.
The GEO dropzone has a subdirectory for the meta.xml, all other dropzones should provide an empty string here.

### `update_datasets`

Boolean flag (true/false) to determine if this harvester should update existing datasets or not.
If the flag is `false` no updates will be performed, only new datasets will be added.

### `update_date_last_modified`

Boolean flag (true/false) to determine if the field `date_last_modified` of a dataset should be updated by the harvester or not.
If the flag is `true` the date will be updated if the content of any resource of a dataset has changed.

## Metadata

You can find examples for `meta.xml` and `link.xml` files in the [`fixtures` directory of this repository](https://github.com/opendatazurich/ckanext-stadtzh-harvest/tree/master/ckanext/stadtzhharvest/tests/fixtures).

### meta.xml

Each dataset constist of a folder containing a `meta.xml` (required!) and an arbitrary number of resources.

Folder struture:
```
DWH/bev_zuz_jahr_quartier
├── 1993-2012_bev_zuz_jahr_quartier.csv
├── bev_zuz_jahr_quartier.csv
└── meta.xml
```


#### `titel`

* Data type: String
* Cardinality: 1
* Description: Title of the dataset
* Values: any literal
* Example:
```xml
<titel>Alterswohnung</title>
```

#### `anwendungen`

* Data type: complex
* Cardinality: 1
* Description: List of applications related to this dataset
* Values: `<anwendung>` childs (with `<titel>`, `<beschreibung>` and `<url>`)
* Example:
```xml
<anwendungen>
	<anwendung>
		<titel>Stadtplan</titel>
		<beschreibung>Verwendung des Datensatzes im Stadtplan</beschreibung>
		<url>http://www.stadtplan.stadt-zuerich.ch/zueriplan/stadtplan.aspx?2a672998-cfb2-473a-934d-c316e8b01ad3</url>
	</anwendung>
</anwendungen>
```

#### `datentyp`

* Data type: String
* Cardinality: 1
* Description: Type of data in this dataset
* Values: `"   "`, `"Bilddatei"`, `"Datenaggregat"`, `"Einzeldaten"`, `"Web-Service"`
* Example:
```xml
<datentyp>Datenaggregat</datentyp>
```

### link.xml

Optionally a dataset may contain a `link.xml` to describe APIs or services.
This is mostly used in the "GEO" dropzone.

Folder struture:
```
GEO/alterswohnung
├── ARCHIVE
└── DEFAULT
    ├── alterswohnung.json
    ├── alterswohnung.kmz
    ├── alterswohnung.zip
    ├── link.xml
    └── meta.xml
```



