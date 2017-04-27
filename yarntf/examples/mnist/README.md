# MNIST

The orginal sources is:
- https://github.com/yahoo/TensorFlowOnSpark/blob/master/examples/mnist/tf/mnist_dist.py
- https://github.com/tensorflow/tensorflow/blob/master/tensorflow/tools/dist_test/python/mnist_replica.py

This is basicaly a clone of the TensorFlowOnSpark example.

## Try the example

At first, we assume that Python is installed on the cluster. If needed dependencies are not avaliable, they can be added from a local source with the `--files` flag to PYTHONPATH, as a comma-separated list of .zip, .egg, or .py files. For more arguments see `yarntf-submit --help`.

1. To get the data set for this example, TFRecords or CSV, please follow [this guide](https://github.com/yahoo/TensorFlowOnSpark/wiki/GetStarted_YARN).
2. Clone and `mvn install`: https://github.com/hopshadoop/hops-tensorflow
3. Locate _yarntf-submit_ in _hops-tensorflow/bin_.
4. Run distributed training:
```
$HADOOP_HOME/bin/hadoop fs -rm -r mnist_model
$HOPSTF_HOME/bin/yarntf-submit \
        --queue         default \
        --workers       3 \
        --pses          1 \
        --memory        1024 \
        --vcores        1 \
        --main $HOPSTF_HOME/yarntf/examples/mnist/mnist.py \
        --args \
        --images mnist/tfr/train \
        --format tfr \
        --mode train \
        --model mnist_model
```
5. Run distributed inference:
```
$HADOOP_HOME/bin/hadoop fs -rm -r mnist_predictions
$HOPSTF_HOME/bin/yarntf-submit \
        --queue         default \
        --workers       3 \
        --pses          1 \
        --memory        1024 \
        --vcores        1 \
        --main $HOPSTF_HOME/yarntf/examples/mnist/mnist.py \
        --args \
        --images mnist/tfr/test \
        --mode inference \
        --model mnist_model \
        --output mnist_predictions
```
