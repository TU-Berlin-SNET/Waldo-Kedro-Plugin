"""
Generic Database Views
"""

import json
from .models import Base, OutlierScore, Contexts
from sqlalchemy_utils import create_materialized_view
from sqlalchemy import Table, select
from datetime import datetime
from .plugin import hooks
from enum import Enum


class ADAlgorithms(Enum):
    IsolationForest = "Isolation Forest"
    EllipticEnvelope = "Elliptic Envelope"
    LocalOutlierFactor = "Local Outlier Factor"


def create_samples_os_view(algo: ADAlgorithms, params: dict):
    samples: Table = Base.metadata.tables.get("samples")
    samples_os = select(samples, Contexts.run_id, Contexts.algorithm, Contexts.parameters, OutlierScore)\
        .join(OutlierScore, samples.c.id == OutlierScore.sample_id)\
        .join(Contexts, OutlierScore.context_id == Contexts.id)\
        .where(
            Contexts.run_id == str(hooks.trace_id),
            Contexts.algorithm == algo.value,
            Contexts.parameters == json.dumps(params),
        )
    current_timestamp: str = str(datetime.now().timestamp())
    create_materialized_view(current_timestamp, samples_os, Base.metadata)
