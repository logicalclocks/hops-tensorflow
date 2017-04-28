# TensorFlow-Slim

Original source: https://github.com/tensorflow/models/tree/master/slim

## Try the example, CIFAR10 data set

1. Prepare the data set and upload it to HDFS at `${SLIM_CIFAR10}`, according to the instructions in the original 
source.
2. Zip Slim files:
```
pushd ${HOPSTF_HOME}/yarntf/examples/slim; rm slim.zip; zip -r slim.zip .; popd
```
3. Run training:
```
${HADOOP_HOME}/bin/hadoop fs -rm -r slim_train
${HOPSTF_HOME}/bin/yarntf-submit \
        --workers       2 \
        --pses          1 \
        --memory        8192 \
        --vcores        1 \
        --gpus          1 \
        --files ${HOPSTF_HOME}/yarntf/examples/slim/slim.zip \
        --main ${HOPSTF_HOME}/yarntf/examples/slim/train_image_classifier.py \
        --args \
        --dataset_dir ${SLIM_CIFAR10} \
        --train_dir hdfs://default/user/${USER}/slim_train \
        --dataset_name cifar10 \
        --dataset_split_name train \
        --model_name inception_v3 \
        --max_number_of_steps 1000
```
4. Run evaluation:
```
${HADOOP_HOME}/bin/hadoop fs -rm -r slim_eval
${HOPSTF_HOME}/bin/yarntf-submit \
        --workers       1 \
        --pses          0 \
        --memory        8192 \
        --vcores        1 \
        --gpus          1 \
        --files ${HOPSTF_HOME}/yarntf/examples/slim/slim.zip \
        --main ${HOPSTF_HOME}/yarntf/examples/slim/eval_image_classifier.py \
        --args \
        --dataset_dir ${SLIM_CIFAR10} \
        --dataset_name cifar10 \
        --dataset_split_name validation \
        --model_name inception_v3 \
        --checkpoint_path hdfs://default/user/${USER}/slim_train \
        --eval_dir hdfs://default/user/${USER}/slim_eval
```

## Note
This TensorFlow-Slim example suffers some issues since the TF v1 release.
