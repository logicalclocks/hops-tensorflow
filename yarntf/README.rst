yarntf
======

``yarntf`` simplifies the distributed TensorFlow submit model, for running
machine learning applications on Hadoop YARN clusters.

User Guide
----------

In general it is as simple as follows.

1. In your code: replace ``tf.train.ClusterSpec()`` and ``tf.train.Server()`` with ``yarntf.createClusterServer()``
2. On your cluster: submit the application with `Hops-TensorFlow <https://github.com/hopshadoop/hops-tensorflow>`_

Your ClusterSpec is generated automaticaly and the parameter servers stopped when all workers are completed. Specify the number of worker, ps and resources on submit.

For more details see the examples.

Work In Progress
----------------

Development is still in an early stage. Contributions are very welcome!

License
-------

``yarntf`` and ``Hops-TensorFlow`` is released under an Apache 2.0 license.
