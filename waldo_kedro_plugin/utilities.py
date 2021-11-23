"""
Utility functions for the plugin
"""

import hashlib
import uuid

import math
from kedro_viz.data_access import data_access_manager
from kedro_viz.services import layers_services
from datetime import datetime
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from typing import Dict, List, Tuple
from sqlalchemy.orm import Session
from kedro.config import ConfigLoader
import sqlalchemy
import logging
from .models import Events, Pipelines, Catalogs, Base, Contexts

# Changing the default level of the root logger to INFO level, to see it printed to console
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Will create the engine globally just once
conf_paths = ["conf/base"]
conf_loader = ConfigLoader(conf_paths)
# Connects to in-memory sqlite db if catalog configuration file cannot be accessed
conn_str = "sqlite://"
try:
    conf_cred = conf_loader.get("credentials*")
    conn_str = conf_cred["postgres"]["con"]
except ValueError as err:
    logging.error(err)

Engine = sqlalchemy.create_engine(conn_str, future=True)


def calc_hash(value: str) -> str:
    """
    calculate the hash to be used for `after_catalog_created` and `before_pipeline_run`
    :param value: string value to calculate the hash for
    :return: hash string
    """
    return hashlib.sha1(value.encode("UTF-8")).hexdigest()[:8]


def partition_indexes(total_size: int, chunk_size: int) -> List[Tuple[int, int]]:
    """
    given the total_size of the list or a dataframe and the size of the chunks, it returns the list of tuples,
    where each tuple contains the starting and ending index for each computed chunk inside a list or a dataframe
    :param total_size: length of a list or size of a dataframe
    :param chunk_size: size of the chunk
    :return: list of tuples for chunk's start and end indices
    """
    # Compute the chunk size (integer division; i.e. assuming Python 2.7)
    num_of_partitions = math.floor(total_size / chunk_size)
    # How many chunks need an extra 1 added to the size?
    remainder = total_size - chunk_size * num_of_partitions
    a = 0
    indexes = []
    for i in range(num_of_partitions):
        b = a + chunk_size + (i < remainder)
        # Yield the inclusive-inclusive range
        indexes.append((a, b - 1))
        a = b

    return indexes


def insert_event(session, run_id: uuid, event_type: str, target_id: str, target_name: str) -> Events:
    """
    method to insert values to the events table
    :param session: database session
    :param run_id: run id
    :param event_type: event type
    :param target_id: target id
    :param target_name: target name
    :return: models.Events
    """
    try:
        new_event = Events(
            run_id=run_id,
            event_type=event_type,
            target_id=calc_hash(target_id),
            target_name=target_name,
            timestamp=datetime.now(),
        )
        session.merge(new_event)
        session.commit()

        return new_event
    except sqlalchemy.exc.SQLAlchemyError as err:
        logging.error(err)
        session.rollback()
        return None


def insert_catalog(session, target: str) -> Catalogs:
    """
    method to insert values to the events table
    :param session: database session
    :param target: target
    :return: models.Catalogs
    """
    try:
        new_catalog = Catalogs(hash=calc_hash(target), content=target)
        session.merge(new_catalog)
        session.commit()

        return new_catalog
    except sqlalchemy.exc.SQLAlchemyError as err:
        logging.error(err)
        session.rollback()
        return None


def insert_pipeline(session, target: str, name: str, content: str) -> Pipelines:
    """
    method to insert values to the events table
    :param session: database session
    :param target: target
    :param name: name
    :param content: content
    :return: models.Pipelines
    """
    try:
        new_pipeline = Pipelines(hash=calc_hash(target), name=name, content=content)
        session.merge(new_pipeline)
        session.commit()

        return new_pipeline
    except sqlalchemy.exc.SQLAlchemyError as err:
        logging.error(err)
        session.rollback()
        return None


def insert_context(session, run_id: str, algorithm: str, parameters: str) -> Contexts:
    """
        method to insert values to the events table
        :param session: database session
        :param run_id: run_id
        :param algorithm: algorithm
        :param parameters: parameters
        :return: models.Contexts
        """
    try:
        new_context = Contexts(run_id=run_id, algorithm=algorithm, parameters=parameters)
        session.add(new_context)
        session.commit()

        return new_context
    except sqlalchemy.exc.SQLAlchemyError as err:
        logging.error(err)
        session.rollback()
        exit(-1)


def emit_ddl(session) -> None:
    """
    emits the db schema created inside sqlalchemy metadata, to the database
    :param session: database session
    :return: None
    """
    engine = session.get_bind()
    Base.metadata.create_all(engine)


def populate_data(catalog: DataCatalog, pipelines: Dict[str, Pipeline]) -> None:
    """
    Populates after parsing kedro pipelines and catalog.
    :param catalog: kedro data catalog
    :param pipelines: kedro pipeline
    :return: None
    """
    data_access_manager.add_catalog(catalog)
    data_access_manager.add_pipelines(pipelines)
    data_access_manager.set_layers(
        layers_services.sort_layers(
            data_access_manager.nodes.as_dict(),
            data_access_manager.node_dependencies,
        )
    )


def get_session() -> Session:
    """
    Creates an sqlalchemy session using the connection string from the kedro project catalog configuration.
    If the catalog configuration does not exist, it creates a session with an in memory empty sqlite database instead
    :return: sqlalchemy.orm.Session
    """
    return Session(Engine)
