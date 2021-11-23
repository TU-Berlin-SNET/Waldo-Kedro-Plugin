"""
Hooks for the plugin
"""

import json
import logging
from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline.node import Node
from kedro.pipeline import Pipeline
from typing import Dict, Any
import inspect
from kedro_viz.api import responses
from .utilities import (
    populate_data,
    insert_event,
    insert_catalog,
    insert_pipeline,
    get_session,
    emit_ddl,
)
from . import gateway
import uuid


class MyHooks:
    """
    Custom Hook class of the Waldo Kedro Plugin
    """

    def __init__(self):
        self.trace_id = uuid.uuid1()

    @hook_impl
    def after_catalog_created(self, conf_catalog: Dict[str, Any]) -> None:
        """
        Hook to store information on DB after the catalog is created. It writes `trace_id`, name of this method and
        the content of the `conf_catalog` to the `events` table. Additionally this also writes hash of a catalog,
        pipeline name and the hash of `conf_catalog` to catalogs table.
        :param conf_catalog: catalog configuration provided by kedro @hook_spec
        :return: None
        """
        target = json.dumps(conf_catalog, sort_keys=True)
        logging.info(f"Catalog {target} loaded")

        with get_session() as session:
            insert_event(session, self.trace_id, inspect.stack()[0][3], target, None)
            insert_catalog(session, target)

    @hook_impl
    def before_node_run(self, node: Node, inputs: Dict[str, Any]) -> None:
        """
        Hook to store information on DB before a node is run. This writes `trace_id`, name of this method and
        `node` to the `events` table.
        :param node: node information provided by kedro @hook_spec
        :param inputs: The dictionary of inputs dataset provided by kedro @hook_spec
        :return: None
        """

        gateway.validate_node_input(node, inputs)
        logging.info(f"Running node {node.name}")

        with get_session() as session:
            insert_event(session, self.trace_id, inspect.stack()[0][3], node.name, None)

    @hook_impl
    def after_node_run(self, node: Node, outputs: Dict[str, Any]) -> None:
        """
        Hook to store information on DB after a node is run. This writes `trace_id`, name of this method and
        `node` to the `events` table.
        :param node: node information provided by kedro @hook_spec
        :param outputs: The dictionary of outputs dataset provided by kedro @hook_spec
        :return: None
        """

        gateway.validate_node_output(node, outputs)
        logging.info(f"node {node.name} run successfully")

        with get_session() as session:
            insert_event(session, self.trace_id, inspect.stack()[0][3], node.name, None)

    @hook_impl
    def on_node_error(self, node: Node) -> None:
        """
        Hook to store information on DB if a node fails. This writes `trace_id`, name of this method and `node`
         to the `events` table.
        :param node: node information provided by kedro @hook_spec
        :return: None
        """
        logging.info(f"running node {node.name} failed")
        with get_session() as session:
            insert_event(session, self.trace_id, inspect.stack()[0][3], node.name, None)

    @hook_impl
    def before_pipeline_run(
        self, run_params: Dict, pipeline: Pipeline, catalog: DataCatalog
    ) -> None:
        """
        Hook to store information on DB before a pipeline is run. This writes `trace_id`, name of this method and
        `dataset_name` to the `events` table. Additionally this also writes hash of a pipeline, pipeline name and the
        content of the data catalog to the `pipelines` table
        :param run_params: run parameters provided by kedro @hook_spec.
        :param pipeline: kedro pipeline
        :param catalog: kedro catalog
        :return: None
        """
        pipeline_name: str = run_params["pipeline_name"]
        if pipeline_name is None:
            pipeline_name = "__default__"

        logging.info(f"before running pipeline: {pipeline}")

        pipelines = {pipeline_name: pipeline}
        populate_data(catalog, pipelines)
        res = responses.get_default_response()
        structure: str = res.json(sort_keys=True)

        with get_session() as session:
            insert_event(
                session, self.trace_id, inspect.stack()[0][3], str(pipeline), None
            )
            insert_pipeline(session, str(pipeline), pipeline_name, structure)

    @hook_impl
    def after_pipeline_run(self, pipeline: Pipeline) -> None:
        """
        Hook to store information on DB after a pipeline is run. This writes `trace_id`, name of this method and
        `pipeline` to the `events` table.
        :param pipeline: pipeline information provided by kedro @hook_spec
        :return: None
        """

        with get_session() as session:
            insert_event(
                session, self.trace_id, inspect.stack()[0][3], str(pipeline), None
            )

            # Emit created materialized views to the db during this run
            emit_ddl(session)

        logging.info(f"pipeline {pipeline} run successfully")

    @hook_impl
    def on_pipeline_error(self, pipeline: Pipeline) -> None:
        """
        Hook to store information on DB if a pipeline fails. This writes `trace_id`, name of this method and
        `pipeline` to the `events` table.
        :param pipeline: pipeline information provided by kedro @hook_spec
        :return: None
        """

        with get_session() as session:
            insert_event(
                session, self.trace_id, inspect.stack()[0][3], str(pipeline), None
            )

        logging.info(f"running pipeline {pipeline} failed")

    @hook_impl
    def before_dataset_loaded(self, dataset_name: str) -> None:
        """
        Hook to store information on DB before datasets are loaded. This writes `trace_id`, name of this method and
        `dataset_name` to the `events` table.
        :param dataset_name: name of the dataset provided in data catalog.
        :return: None
        """
        logging.info(f"loading dataset {dataset_name}")

        with get_session() as session:
            insert_event(
                session,
                self.trace_id,
                inspect.stack()[0][3],
                dataset_name,
                dataset_name,
            )

    @hook_impl
    def after_dataset_loaded(self, dataset_name: str) -> None:
        """
        Hook to store information on DB after datasets are loaded. This writes `trace_id`, name of this method and
        `dataset_name` to the `events` table.
        :param dataset_name: name of the dataset provided in data catalog.
        :return: None
        """
        logging.info(f"dataset {dataset_name} loaded successfully")

        with get_session() as session:
            insert_event(
                session,
                self.trace_id,
                inspect.stack()[0][3],
                dataset_name,
                dataset_name,
            )

    @hook_impl
    def before_dataset_saved(self, dataset_name: str) -> None:
        """
        Hook to store information on DB before datasets are saved. This writes `trace_id`, name of this method and
        `dataset_name` to the `events` table.
        :param dataset_name: name of the dataset provided in data catalog.
        :return: None
        """
        logging.info(f"saving dataset {dataset_name}")

        with get_session() as session:
            insert_event(
                session,
                self.trace_id,
                inspect.stack()[0][3],
                dataset_name,
                dataset_name,
            )

    @hook_impl
    def after_dataset_saved(self, dataset_name: str) -> None:
        """
        Hook to store information on DB after datasets are saved. This writes `trace_id`, name of this method and
        `dataset_name` to the `events` table.
        :param dataset_name: name of the dataset provided in data catalog.
        :return: None
        """
        logging.info(f"dataset {dataset_name} saved successfully")

        with get_session() as session:
            insert_event(
                session,
                self.trace_id,
                inspect.stack()[0][3],
                dataset_name,
                dataset_name,
            )


hooks = MyHooks()
