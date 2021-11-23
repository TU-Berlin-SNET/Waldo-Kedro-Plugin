# Waldo Kedro Plugin

This is a kedro plugin that writes information from kedro hooks into a database.

## Installation

#### For development:

Activate the virtual environment in your project with `venv\Scripts\activate` and:

```
pip install -e $PATH_TO_PLUGIN_PROJECT

```

**For Example,  `pip install -e ~/waldo-kedro-plugin/`**

`pip -e --editable` let us edit the code for the package without having to re-install the package every time. It will technically not install the package but will create a `.egg-link` in the deployment directory back to the project source code directory, meaning instead of copying to the `site-packages` it adds a symbolic link `.egg-link`.


#### If you don't intend to edit the plugin:

Install it from Test PyPI:

```
pip install -i https://test.pypi.org/simple/ waldo-kedro-plugin
```

## Available hooks

All the hook specifications provided in [kedro.framework.hooks](https://kedro.readthedocs.io/en/latest/07_extend_kedro/02_hooks.html#execution-timeline-hooks) are Available. The names are self explanatory.

- `after_catalog_created`
- `before_node_run`
- `after_node_run`
- `on_node_error`
- `before_pipeline_run`
- `after_pipeline_run`
- `on_pipeline_error`
- `before_dataset_loaded`
- `after_dataset_loaded`
- `before_dataset_saved`
- `after_dataset_saved`

## How to use the plugin

After you install the plugin, kedro automatically detects it. You do not need to do anything because hook implementations are automatically registered to the project context when the [plugin](https://kedro.readthedocs.io/en/stable/07_extend_kedro/04_plugins.html#hooks) is installed. However, the data schema must match otherwise the writing to database will fail.

## Database schema

This plugin makes use of following three tables:

**catalogs:**

| column | hash 🔑| content |
| ------ | ------ | ------- |
| Type   | varchar(8)| json |


**events:**

| column | id 🔑  | run_id   | event_type | target_id | target_name | timestamp |
| ------ | ------ | ------- | ------ | ------ | ------ | ------ |
| Type   | bigint | char(36) | text | varchar(8) | text | timestamp |


**pipelines:**

| column | hash 🔑| name | content |
| ------ | ------  | ------- | ------- |
| Type   | varchar(8)| text | json |

**samples:**

| column | id 🔑| --- |
| ------ | ------  | ------- |
| Type   | bigint| --- |

**contexts:**

| column | id 🔑| run_id | algorithm | parameters |
| ------ | ------ | ------- | ------ | ------ |
| Type   | int | char(36) | text | text |

**outlier_score:**

| column | context_id 🔑| sample_id 🔑| score | prediction |
| ------ | ------ | ------- | ------ | ------ |
| Type   | int | bigint | float | boolean |

_Note: `samples` table has only one hard constraint,i.e, it must contain a column named ``id``, which can serve as a foreign key to the generic table ``outlier_score``.

All hooks write into `events` table, whereas, only `after_catalog_created` writes into `catalogs` table.
On the other hand, only `before_pipeline_run` writes into `pipelines` table.


## Requirements

All the required packages will be installed when you install the plugin, however, there are certain things you need to consider.

- We are using postgresql. Therefore, you need to have a postgres server running. Please create an empty database and 
provide the credentials to access it in a `credentials.yml` file in a kedro project in this format.

    ```
    postgres:
        con: postgresql://$USER_NAME:$PASSWORD@$SERVER_NAME:$PORT/$DB_NAME
    ```

- Database can have any name, as long as it is correctly provided in `credentials.yml` file.
- All the required tables will be created inside the database automatically, once you will run the project for the first time.
