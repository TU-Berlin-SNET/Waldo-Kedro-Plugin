"""
As a framework, Waldo aims to provide interchangeable, easy to use modules for anomaly detection.
This requirement is reflected in the database schema, which in effect is a deliberate constraint of the Kedro design
space.

- Waldo application data will be stored in a relational SQL database with predefined referencing between tables.
- Each application database consists of at least three tables ``contexts``, ``samples`` and ``outlier_score``, where
``contexts`` and ``outlier_score`` are generic tables shared by all Waldo projects, and ``samples`` is implemented
according to specific use case requirements.

**Table Structure**

``samples``
    With regard to the ``samples`` table there is only one hard constraint in that it must contain a column named ``id``
    which can serve as a foreign key to the generic table ``outlier_score``. Apart from that, the table is specified on a per
    use case basis, holding the pre-processed feature set (to be used for anomaly detection) as well as a optional number of
    columns that may not factor in to AD tasks directly but rather provide context to the end user.

``contexts``
    +----------------+--------------+-----------+
    | Column name    | Data type    | Notes     |
    +================+==============+===========+
    | *id*           | ``INT``      | PK        |
    +----------------+--------------+-----------+
    | *run_id*       | ``CHAR(36)`` | reserved  |
    +----------------+--------------+-----------+
    | *algorithm*    | ``TEXT``     | short ID  |
    +----------------+--------------+-----------+
    | *parameters*   | ``TEXT``     |           |
    +----------------+--------------+-----------+

    The inclusion of algorithm identifiers and parameters enable parallel analysis using multiple methods for AD.
    By separating the result data from the samples we reduce SQL update statements and avoid conflicts on consecutive
    runs.

``outlier_score``
    +----------------+--------------+-----------+
    | Column name    | Data type    | Notes     |
    +================+==============+===========+
    | *context_id*   | ``INT``      | FK, PK    |
    +----------------+--------------+-----------+
    | *sample_id*    | ``BIGINT``   | FK, PK    |
    +----------------+--------------+-----------+
    | *score*        | ``FLOAT``    | index     |
    +----------------+--------------+-----------+
    | *prediction*   | ``BOOLEAN``  |           |
    +----------------+--------------+-----------+
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

from sqlalchemy.orm import declarative_base
from sqlalchemy import CHAR, Column, INT, BIGINT, TEXT, VARCHAR, TIMESTAMP, JSON, BOOLEAN, FLOAT, ForeignKey, Index

Base = declarative_base()


class Events(Base):
    """Waldo Events Table (for future use)"""
    __tablename__ = "events"

    id = Column(BIGINT, nullable=False, primary_key=True, autoincrement=True)
    run_id = Column(CHAR(36), nullable=False)
    event_type = Column(TEXT, nullable=False)
    target_id = Column(VARCHAR(8), nullable=False)
    target_name = Column(TEXT, nullable=True)
    timestamp = Column(TIMESTAMP(True), nullable=False)


class Catalogs(Base):
    """Waldo Catalogs Table (for future use)"""
    __tablename__ = "catalogs"

    hash = Column(VARCHAR(8), nullable=False, primary_key=True)
    content = Column(JSON, nullable=False)


class Pipelines(Base):
    """Waldo Pipelines Table (for future use)"""
    __tablename__ = "pipelines"

    hash = Column(VARCHAR(8), nullable=False, primary_key=True)
    name = Column(TEXT, nullable=False)
    content = Column(JSON, nullable=False)


class OutlierScore(Base):
    """Waldo OutlierScore Table"""
    __tablename__ = "outlier_score"

    context_id = Column(INT, ForeignKey('contexts.id'), nullable=False, primary_key=True)
    sample_id = Column(BIGINT, ForeignKey('samples.id'), nullable=False, primary_key=True)
    score = Column(FLOAT, nullable=False)
    prediction = Column(BOOLEAN)

    # place an index on score
    __table_args__ = (
        Index('idx_os_score', "score"),
    )


class Contexts(Base):
    """Waldo Contexts Table"""
    __tablename__ = "contexts"

    id = Column(INT, nullable=False, primary_key=True, autoincrement=True)
    run_id = Column(CHAR(36), nullable=False)
    algorithm = Column(TEXT, nullable=False)
    parameters = Column(TEXT, nullable=False)
