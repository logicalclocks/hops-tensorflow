# CIFAR-10

Original source: https://github.com/tensorflow/models/tree/master/tutorials/image/cifar10

## Try the example

1. Prepare the data set and upload it to HDFS at `${CIFAR10_DATA}`, according to the instructions in the original 
source.
2. Zip CIFAR-10 files:
```
pushd ${HOPSTF_HOME}/yarntf/examples/cifar10; rm cifar10.zip; zip -r cifar10.zip .; popd
```
3. Run training:
```
${HADOOP_HOME}/bin/hadoop fs -rm -r cifar10_train
${HOPSTF_HOME}/bin/yarntf-submit \
        --workers       1 \
        --pses          0 \
        --memory        1024 \
        --vcores        1 \
        --gpus          1 \
        --files ${HOPSTF_HOME}/yarntf/examples/cifar10/cifar10.zip \
        --main ${HOPSTF_HOME}/yarntf/examples/cifar10/cifar10_train.py \
        --args \
        --data_dir ${CIFAR10_DATA} \
        --train_dir hdfs://default/user/${USER}/cifar10_train \
        --max_steps 1000
```
4. Run evaluation:
```
${HOPSTF_HOME}/bin/yarntf-submit \
        --workers       1 \
        --pses          0 \
        --memory        1024 \
        --vcores        1 \
        --gpus          1 \
        --files ${HOPSTF_HOME}/yarntf/examples/cifar10/cifar10.zip \
        --main ${HOPSTF_HOME}/yarntf/examples/cifar10/cifar10_train.py \
        --args \
        --data_dir ${CIFAR10_DATA} \
        --checkpoint_dir hdfs://default/user/${USER}/cifar10_train \
        --eval_dir hdfs://default/user/${USER}/cifar10_eval \
        --run_once
```
