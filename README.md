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

* **Data type**: String
* **Cardinality**: 1
* **Description**: Title of this dataset
* **Values**: any literal
* **Example**:
```xml
<titel>Alterswohnung</title>
```

#### `beschreibung`

* **Data type**: String
* **Cardinality**: 1
* **Description**: Description of this dataset
* **Values**: any literal
* **Example**:
```xml
<beschreibung>Liste der städtischen Alterswohnungen</beschreibung>
```

#### `rechtsgrundlage`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Description of the legal basis of this dataset
* **Values**: any literal
* **Example**:
```xml
<rechtsgrundlage>Stadtratsbeschluss DGA</rechtsgrundlage>
```

#### `raeumliche_beziehung`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Spatial relationship of this dataset
* **Values**: any literal
* **Example**:
```xml
<raeumliche_beziehung>Stadt Zürich</raeumliche_beziehung>
```

#### `aktualisierungsintervall`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Update interval of this dataset
* **Values**: `"   "`, `"alle 4 Jahre"`, `"Echtzeit"`, `"halbjaehrlich"`, `"jaehrlich"`, `"keines"`, `"laufend"`, `"monatlich"`, `"quartalsweise"`, `"sporadisch oder unregelmaessig"`, `"stuendlich"`, `"taeglich"`, `"vierzehntaeglich"`, `"woechentlich"`, `"laufende Nachfuehrung"`, `"keine Nachfuehrung"`
* **Example**:
```xml
<aktualisierungsintervall>woechentlich</aktualisierungsintervall>
```

#### `aktualisierungsdatum`

* **Data type**: String
* **Cardinality**: 1
* **Description**: Date of the last update of this dataset
* **Values**: date `dd.mm.yyyy`
* **Example**:
```xml
<aktualisierungsdatum>20.10.2015</aktualisierungsdatum>
```

#### `datentyp`

* **Data type**: String
* **Cardinality**: 1
* **Description**: Type of data in this dataset
* **Values**: `"   "`, `"Bilddatei"`, `"Datenaggregat"`, `"Einzeldaten"`, `"Web-Service"`
* **Example**:
```xml
<datentyp>Datenaggregat</datentyp>
```

#### `erstmalige_veroeffentlichung`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Publication date of this dataset
* **Values**: `dd.mm.yyyy`
* **Example**:
```xml
<erstmalige_veroeffentlichung>21.01.2015</erstmalige_veroeffentlichung>
```

#### `kategorie`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: List of categories of this dataset
* **Values**: comma-seperated list of category-titles
* **Example**:
```xml
<kategorie>Basiskarten, Bevölkerung, Bauen und Wohnen</kategorie>
```

#### `lieferant`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Publisher of this dataset
* **Values**: any literal
* **Example**:
```xml
<lieferant>Geomatik und Vermessung Zürich, Tiefbau- und Entsorgungsdepartement</lieferant>
```

#### `zeitraum`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Temporal relationship of this dataset
* **Values**: any literal
* **Example**:
```xml
<zeitraum>laufende Nachführung</zeitraum>
```

#### `quelle`

* **Data type**: String
* **Cardinality**: 1
* **Description**: Source of this dataset
* **Values**: any literal
* **Example**:
```xml
<quelle>Gesundheits- und Umweltdepartement</quelle>
```

#### `datenqualitaet`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Quality of this dataset
* **Values**: any literal
* **Example**:
```xml
<datenqualitaet>gut</datenqualitaet>
```

#### `aktuelle_version`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: Version of this dataset
* **Values**: version number
* **Example**:
```xml
<aktuelle_version>1.0</aktuelle_version>
```

#### `bemerkungen`

* **Data type**: complex
* **Cardinality**: 0..1
* **Description**: List of comments related to this dataset
* **Values**: `<bemerkung>` childs (with `<titel>`, `<text>` and `<link>` (with `<label>` and `<url>`))
* **Example**:
```xml
<bemerkungen>
    <bemerkung>
        <titel>Vorschau WMS (statisches Bild):</titel>
        <text>Es werden nur Wohnungen angezeigt, welche mindestens drei Mal vorkommen</text>
        <link>
          <label>Layer Alterswohnung</label>
          <url><![CDATA[http://www.gis.stadt-zuerich.ch/maps/services/wms/WMS-ZH-STZH-OGD/MapServer/WMSServer?VERSION=1.3.0&REQUEST=GetMap&CRS=EPSG:21781&Styles=&FORMAT=image/png&BGCOLOR=0xFFFFFF&TRANSPARENT=FALSE&bbox=676000,241000,690000,255000&WIDTH=800&HEIGHT=800&Layers=Stadtplan,Alterswohnung"]]></url>
      </link>
    </bemerkung>
</bemerkungen>
```

#### `attributliste`

* **Data type**: complex
* **Cardinality**: 0..1
* **Description**: List of attributes to describe the fields of this dataset
* **Values**: `<attributelement technischerfeldname="{fieldname}">` childs (with `<sprechenderfeldname>` and `<feldbeschreibung>`)
* **Example**:
```xml
<attributliste> 
   <attributelement technischerfeldname="ADRESSE">
        <sprechenderfeldname>Adresse</sprechenderfeldname>
        <feldbeschreibung>Adresse des Objektes, CHAR 70 Zeichen lang</feldbeschreibung>
   </attributelement>
</attributliste>
```

#### `schlagworte`

* **Data type**: String
* **Cardinality**: 0..1
* **Description**: List of tags of this dataset
* **Values**: comma-seperated list of tag-titles
* **Example**:
```xml
<schlagworte>geodaten, vektordaten, punktdaten, standort</schlagworte>
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

Full example of a link.xml:

```xml
<?xml version="1.0" encoding="utf-8"?>
<linklist>
	<link>
		<lable>Web Map Service</lable>
		<url><![CDATA[http://www.gis.stadt-zuerich.ch/maps/services/wms/WMS-ZH-STZH-OGD/MapServer/WMSServer?]]></url>
		<type>WMS</type>
	</link>
	<link>
		<lable>Web Feature Service</lable>
		<url><![CDATA[http://www.gis.stadt-zuerich.ch/maps/services/wms/WMS-ZH-STZH-OGD/MapServer/WFSServer?]]></url>
		<type>WFS</type>
	</link>
</linklist>
```

#### `link`

* **Data type**: complex
* **Cardinality**: 0..n
* **Description**: Describes resources that are available as a URL (APIs, services)
* **Values**: Elements `<lable>` (name of the resource, _required_), `<url>` (URL of the resource, _required_), `<type>` (format of the resource, _required_), `<description>` (description of the resource, _optional_)
* Example:
```xml
<link>
    <lable>Web Feature Service</lable>
    <url><![CDATA[http://www.gis.stadt-zuerich.ch/maps/services/wms/WMS-ZH-STZH-OGD/MapServer/WFSServer?]]></url>
    <type>WFS</type>
    <description>Projektion CH1903+ / LV95 (EPSG:2056)</description>
</link>
```
