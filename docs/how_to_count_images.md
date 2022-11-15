---
jupytext:
  text_representation:
    format_name: myst
kernelspec:
  display_name: Python 3
  name: python3
---

# How to: Sumarize ImageMetadata

+++

```{note}
This example requires a running instance of `cobra_db`.
This example runs on the one that was created in the first tutorial.
```

In this example, we will summarize the ImageMetadata collection that was created in the
first tutorial. The same code can be used with any database that you created in the same
way.

```{code-cell} ipython3
import cobra_db
print(f"This example was run with cobra_db v{cobra_db.__version__}")
```

```{code-cell} ipython3
:tags: [remove-cell]
# This cell should not be visible in the documentation
import os
from dotenv import load_dotenv
pwd = './'
dotenv_path = os.path.join(pwd, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
my_mongo_username = os.environ['docs_user']
my_db_name = os.environ['docs_db_name']
my_mongo_password = os.environ['docs_mongo_pass']
my_mongo_host = os.environ['mongo_host']
```

```{code-cell} ipython3
from cobra_db import Connector

# Create the connector with your own credentials.
# Remember that you can also use
# `Connector.get_env_pass` or `Connector.get_pass`
# to keep your password safe.
connector = Connector(
    host=my_mongo_host,
    db_name=my_db_name,
    username=my_mongo_username,
    password=my_mongo_password,
    port=27017  # In case you need to specify one
)
```

Now let's list all the SOPClasses that exist in our `ImageMetadata` collection
```{code-cell} ipython3
from cobra_db import ImageMetadataDao
# create the data access object
im_dao = ImageMetadataDao(connector)
# prepare the aggregation pipeline.
pipeline = [
    {
        "$group": {
            "_id": {"$first": "$dicom_tags.SOPClassUID.Value"},
            "n": {"$sum": 1},
        }
    },
    {"$sort": {"n": -1}},
]
sop_classes = list(im_dao.collection.aggregate(pipeline, allowDiskUse=True))

print(sop_classes)
```

This is already useful, but let's display it as a pandas dataframe with readable names.

```{code-cell} ipython3
import pandas as pd
from pydicom._uid_dict import UID_dictionary
for i, c in enumerate(sop_classes):
    sop_classes[i]['sop_class_name'] = UID_dictionary.get(c['_id'], ['Unknown'])[0]
pd.DataFrame(sop_classes)

```

Let's now group the instances of each SOP Class by different tags.
Grouping by `SOPInstanceUID` allows us to see how many duplicate files we have. Grouping by `SeriesInstanceUID`, `StudyInstanceUID`, and `PatientID` is also insightful.

```{code-cell} ipython3
# we define a funtion that will be used with each SOP Class
def analyse_sop_class(uid):
    class_name = UID_dictionary.get(uid, ["Unknown"])[0]
    n_images = im_dao.collection.count_documents({"dicom_tags.SOPClassUID.Value": uid})

    def group_and_count(tag: str):
        return list(
            im_dao.collection.aggregate(
                [
                    {"$match": {"dicom_tags.SOPClassUID.Value": uid}},
                    {
                        "$group": {
                            "_id": {"$first": f"$dicom_tags.{tag}.Value"},
                            "n": {"$sum": 1},
                        },
                    },
                    {
                        "$facet": {  # split the pipeline to count different things
                            f"n_{tag}": [{"$count": "n"}],
                            f"min_n_images_per_{tag}": [
                                {"$sort": {"n": 1}},
                                {"$limit": 1},
                            ],
                            f"max_n_images_per_{tag}": [
                                {"$sort": {"n": -1}},
                                {"$limit": 1},
                            ],
                        }
                    },
                    {
                        "$project": {
                            f"n_{tag}": {"$first": f"$n_{tag}.n"},
                            f"min_n_images_per_{tag}": {
                                "$first": f"$min_n_images_per_{tag}.n"
                            },
                            f"max_n_images_per_{tag}": {
                                "$first": f"$max_n_images_per_{tag}.n"
                            },
                        }
                    },
                ],
                allowDiskUse=True,
            )
        )[0]

    ans = {"SOPClassUID": uid, "class_name": class_name, "n_images": n_images}
    ans.update(group_and_count("SOPInstanceUID"))
    ans.update(group_and_count("SeriesInstanceUID"))
    ans.update(group_and_count("StudyInstanceUID"))
    ans.update(group_and_count("PatientID"))
    return ans
```

```{code-cell} ipython3
:tags: [output-scroll]
from pprint import pprint
sop_classes_analysis = [analyse_sop_class(doc['_id']) for doc in sop_classes]
pprint(sop_classes_analysis)
```
