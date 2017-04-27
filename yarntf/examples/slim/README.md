# TensorFlow-Slim

Original source: https://github.com/tensorflow/models/tree/master/slim

## Try the example, Flowers data set

1. Prepare the data set and upload it to HDFS at `${FLOWERS_DATA}`, according to the instructions in the original 
source.
2. Zip Slim files:
```
pushd ${HOPSTF_HOME}/yarntf/examples/slim; rm slim.zip; zip -r slim.zip .; popd
```
3. Run training:
```
${HADOOP_HOME}/bin/hadoop fs -rm -r slim_train
${HOPSTF_HOME}/bin/yarntf-submit \
        --workers       1 \
        --pses          0 \
        --memory        8192 \
        --vcores        1 \
        --gpus          0 \
        --files ${HOPSTF_HOME}/yarntf/examples/slim/slim.zip \
        --main ${HOPSTF_HOME}/yarntf/examples/slim/train_image_classifier.py \
        --args \
        --dataset_dir ${FLOWERS_DATA} \
        --train_dir hdfs://default/user/${USER}/slim_train \
        --dataset_name flowers \
        --dataset_split_name train \
        --model_name inception_v3 \
        --max_number_of_steps 1000 \
        --clone_on_cpu \
        --num_ps_tasks 0
```
4. Run evaluation:
```
${HADOOP_HOME}/bin/hadoop fs -rm -r slim_eval
${HOPSTF_HOME}/bin/yarntf-submit \
        --workers       1 \
        --pses          0 \
        --memory        8192 \
        --vcores        1 \
        --gpus          0 \
        --files ${HOPSTF_HOME}/yarntf/examples/slim/slim.zip \
        --main ${HOPSTF_HOME}/yarntf/examples/slim/eval_image_classifier.py \
        --args \
        --dataset_dir ${FLOWERS_DATA} \
        --dataset_name flowers \
        --dataset_split_name validation \
        --model_name inception_v3 \
        --checkpoint_path hdfs://default/user/${USER}/slim_train \
        --eval_dir hdfs://default/user/${USER}/slim_eval
```
