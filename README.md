# <img src="static/img/cobra_db.png" alt="" height="30"/> cobra_db



<center>

[![PyPI version](https://badge.fury.io/py/cobra_db.svg)](https://badge.fury.io/py/cobra_db)
[![Documentation Status](https://readthedocs.org/projects/cobra-db/badge/?version=latest)](https://cobra-db.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/mammoai/cobra-db/branch/main/graph/badge.svg?token=ASQPS89408)](https://codecov.io/gh/mammoai/cobra-db)

<img src="static/img/cobra_db.png" alt="" height="200"/>

**Co**nsolidated **Br**east Cancer **A**nalysis **D**ata**B**ase
</center>




## What is ```cobra_db```?
_cobra_db_ is a python package that allows you to extract DICOM metadata and store it in a MongoDB database. Allowing you to index, transform, and export your medical imaging metadata.

With cobra_db, you will have more visibility of your data enabling you to get more value from your medical imaging studies.

Once the metadata is in the database, you can import other text-based information (csv or json) into a custom collection and then run queries. This allows you to mix and match data extracted from different sources in different formats.

For example, let's say you have 1 million mammography DICOM files and you would like to obtain the path of the files that belong to women scanned at an age of between 40 and 50 years old.

If you had cobra_db, you could run the following query in just a few seconds directly in the mongo shell.

```javascript
db.ImageMetadata.find(
  // filter the data
  {patient_age:{$gt:40, $lte:50}},
  // project it into a flat structure
  {
    patient_id: "$dicom_tags.PatientID.Value"
    drive_name: "$file_source.drive_name",
    rel_path:"$file_source.rel_path",
  })
```
This would return the patient id, the drive name and the relative path (to the drive) for all the files that match the selection criteria.

## Installation
If you already have a working instance of the database, you only need to install the python package.

```bash
$ pip install cobra_db
```

If you would like to create a database from scratch, go ahead and follow the [tutorial](https://cobra-db.readthedocs.io/en/latest/tutorial.html).

## Usage

If you have an `ImageMetadata` instance id that you would like to access from python.

```python
from cobra_db import Connector, ImageMetadataDao

# the _id of the ImageMetadata instance that you want to access
im_id = '62de8e38dc2414586e4ddb25'

# prompt user for password
connector = Connector.get_pass(
  host='my_host.server.com',
  port=27017,
  db_name='cobra_db',
  username='my_user'
)
# connect to the ImageMetadata collection
im_dao = ImageMetadataDao(connector)
im = im_dao.get_by_id(im_id)
print(im.date.file_source.rel_path)

# this will return
... rel/path/to/my_file.dcm
```

## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a [Code of Conduct](https://cobra-db.readthedocs.io/en/latest/conduct.html). By contributing to this project, you agree to abide by its terms.

## License

`cobra_db` was created by Fernando Cossio, Apostolia Tsirikoglou, Annika Gregoorian, Haiko Schurz, and Fredrik Strand. It is licensed under the terms of the Apache License 2.0 license.

## Aknowledgements

This project has been funded by research grants Regional Cancer Centers in Collaboration 21/00060, and Vinnova 2021-0261.
