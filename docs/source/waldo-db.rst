Waldo Database
==============

As a framework, Waldo aims to provide interchangeable, easy to use modules for anomaly detection.
This requirement is reflected in the database schema, which in effect is a deliberate constraint of the Kedro design
space.

- Waldo application data will is stored in a relational SQL database in contrast.
- Each application database consists of at least two tables ``samples`` and ``outlierscore``, where ``outlierscore`` is
  a generic table, shared by all Waldo projects and ``samples`` is implemented according to specific use case
  requirements.

Table Structure
---------------

``samples``
    With regard to the ``samples`` table there is only one hard constraint in that it must contain a column named ``id``
    which can serve as a foreign key to the generic table ``outlierscore``. Apart from that, the table is specified on a per
    use case basis, holding the pre-processed feature set (to be used for anomaly detection) as well as a optional number of
    columns that may not factor in to AD tasks directly but rather provide context to the end user.

``outlierscore``
    +----------------+--------------+-----------+
    | Column name    | Data type    | Notes     |
    +================+==============+===========+
    | ``run_id``     | ``CHAR(36)`` | reserved  |
    +----------------+--------------+-----------+
    | ``sample``     | ``INT``      | FK        |
    +----------------+--------------+-----------+
    | ``score``      | ``FLOAT``    | index     |
    +----------------+--------------+-----------+
    | ``algorithm``  | ``CHAR(36)`` | short ID  |
    +----------------+--------------+-----------+
    | ``parameters`` | ``TEXT``     |           |
    +----------------+--------------+-----------+
    | ``prediction`` | ``BOOLEAN``  |           |
    +----------------+--------------+-----------+

    The inclusion of algorithm identifiers and parameters enable parallel analysis using multiple methods for AD.
    By separating the result data from the samples we reduce SQL update statements and avoid conflicts on consecutive
    runs.