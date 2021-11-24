"""
Generic Database Views
"""

#  Copyright © 2021 Technische Unversität Berlin, Service-centric Networking (SNET) https://snet.tu-berlin.de/
#  Aljoscha Schulte, Christoph Schulthess, Uttam Dhakal, Zohaib Akhtar Khan
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

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
