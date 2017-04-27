"""Distributed MNIST on grid based on TensorFlow MNIST example"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import math
import os
import time
from datetime import datetime

import tensorflow as tf

import yarntf

IMAGE_PIXELS = 28


def print_log(worker_id, arg):
  print(worker_id, end=": ")
  print(arg)


def hdfs_path(relative_path):
  return os.environ["HOME_DIR"] + "/" + relative_path


def main(args):
  job_name = os.environ["JOB_NAME"]
  task_index = int(os.environ["TASK_INDEX"])
  num_workers = int(os.environ["WORKERS"])
  num_pses = int(os.environ["PSES"])
  worker_id = job_name + str(task_index)

  # Parameters
  hidden_units = 128
  batch_size = 100

  # Get TF cluster and server instances
  cluster, server = yarntf.createClusterServer()

  def read_csv_examples(image_dir, label_dir, batch_size=100, num_epochs=None, task_index=None, num_workers=None):
    print_log(worker_id, "num_epochs: {0}".format(num_epochs))
    # Setup queue of csv image filenames
    tf_record_pattern = os.path.join(image_dir, 'part-*')
    images = tf.gfile.Glob(tf_record_pattern)
    print_log(worker_id, "images: {0}".format(images))
    image_queue = tf.train.string_input_producer(images, shuffle=False, capacity=1000, num_epochs=num_epochs,
                                                 name="image_queue")

    # Setup queue of csv label filenames
    tf_record_pattern = os.path.join(label_dir, 'part-*')
    labels = tf.gfile.Glob(tf_record_pattern)
    print_log(worker_id, "labels: {0}".format(labels))
    label_queue = tf.train.string_input_producer(labels, shuffle=False, capacity=1000, num_epochs=num_epochs,
                                                 name="label_queue")

    # Setup reader for image queue
    img_reader = tf.TextLineReader(name="img_reader")
    _, img_csv = img_reader.read(image_queue)
    image_defaults = [[1.0] for col in range(784)]
    img = tf.pack(tf.decode_csv(img_csv, image_defaults))
    # Normalize values to [0,1]
    norm = tf.constant(255, dtype=tf.float32, shape=(784,))
    image = tf.div(img, norm)
    print_log(worker_id, "image: {0}".format(image))

    # Setup reader for label queue
    label_reader = tf.TextLineReader(name="label_reader")
    _, label_csv = label_reader.read(label_queue)
    label_defaults = [[1.0] for col in range(10)]
    label = tf.pack(tf.decode_csv(label_csv, label_defaults))
    print_log(worker_id, "label: {0}".format(label))

    # Return a batch of examples
    return tf.train.batch([image, label], batch_size, num_threads=args.readers, name="batch_csv")

  def read_tfr_examples(path, batch_size=100, num_epochs=None, task_index=None, num_workers=None):
    print_log(worker_id, "num_epochs: {0}".format(num_epochs))

    # Setup queue of TFRecord filenames
    tf_record_pattern = os.path.join(path, 'part-*')
    files = tf.gfile.Glob(tf_record_pattern)
    queue_name = "file_queue"

    # split input files across workers, if specified
    if task_index is not None and num_workers is not None:
      num_files = len(files)
      files = files[task_index:num_files:num_workers]
      queue_name = "file_queue_{0}".format(task_index)

    print_log(worker_id, "files: {0}".format(files))
    file_queue = tf.train.string_input_producer(files, shuffle=False, capacity=1000, num_epochs=num_epochs,
                                                name=queue_name)

    # Setup reader for examples
    reader = tf.TFRecordReader(name="reader")
    _, serialized = reader.read(file_queue)
    feature_def = {'label': tf.FixedLenFeature([10], tf.int64), 'image': tf.FixedLenFeature([784], tf.int64)}
    features = tf.parse_single_example(serialized, feature_def)
    norm = tf.constant(255, dtype=tf.float32, shape=(784,))
    image = tf.div(tf.to_float(features['image']), norm)
    print_log(worker_id, "image: {0}".format(image))
    label = tf.to_float(features['label'])
    print_log(worker_id, "label: {0}".format(label))

    # Return a batch of examples
    return tf.train.batch([image, label], batch_size, num_threads=args.readers, name="batch")

  if job_name == "ps":
    server.join()
  elif job_name == "worker":
    # Assigns ops to the local worker by default.
    with tf.device(tf.train.replica_device_setter(
        worker_device="/job:worker/task:%d" % task_index,
        cluster=cluster)):

      # Variables of the hidden layer
      hid_w = tf.Variable(tf.truncated_normal([IMAGE_PIXELS * IMAGE_PIXELS, hidden_units],
                                              stddev=1.0 / IMAGE_PIXELS), name="hid_w")
      hid_b = tf.Variable(tf.zeros([hidden_units]), name="hid_b")

      # Variables of the softmax layer
      sm_w = tf.Variable(tf.truncated_normal([hidden_units, 10],
                                             stddev=1.0 / math.sqrt(hidden_units)), name="sm_w")
      sm_b = tf.Variable(tf.zeros([10]), name="sm_b")

      # Placeholders or QueueRunner/Readers for input data
      num_epochs = 1 if args.mode == "inference" else None if args.epochs == 0 else args.epochs
      index = task_index if args.mode == "inference" else None
      workers = num_workers if args.mode == "inference" else None

      if args.format == "csv":
        images = hdfs_path(args.images)
        labels = hdfs_path(args.labels)
        x, y_ = read_csv_examples(images, labels, 100, num_epochs, index, workers)
      elif args.format == "tfr":
        images = hdfs_path(args.images)
        x, y_ = read_tfr_examples(images, 100, num_epochs, index, workers)
      else:
        raise ("{0} format not supported for tf input mode".format(args.format))

      hid_lin = tf.nn.xw_plus_b(x, hid_w, hid_b)
      hid = tf.nn.relu(hid_lin)

      y = tf.nn.softmax(tf.nn.xw_plus_b(hid, sm_w, sm_b))

      global_step = tf.Variable(0)

      loss = -tf.reduce_sum(y_ * tf.log(tf.clip_by_value(y, 1e-10, 1.0)))
      train_op = tf.train.AdagradOptimizer(0.01).minimize(
        loss, global_step=global_step)

      # Test trained model
      label = tf.argmax(y_, 1, name="label")
      prediction = tf.argmax(y, 1, name="prediction")
      correct_prediction = tf.equal(prediction, label)
      accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32), name="accuracy")

      saver = tf.train.Saver()
      summary_op = tf.summary.merge_all()
      init_op = tf.global_variables_initializer()

    # Create a "supervisor", which oversees the training process and stores model state into HDFS
    logdir = hdfs_path(args.model)
    print("tensorflow model path: {0}".format(logdir))
    summary_writer = tf.summary.FileWriter(os.environ["TB_DIR"], graph=tf.get_default_graph())

    if args.mode == "train":
      sv = tf.train.Supervisor(is_chief=(task_index == 0),
                               logdir=logdir,
                               init_op=init_op,
                               summary_op=summary_op,
                               saver=saver,
                               global_step=global_step,
                               summary_writer=summary_writer,
                               stop_grace_secs=300,
                               save_model_secs=10)
    else:
      sv = tf.train.Supervisor(is_chief=(task_index == 0),
                               logdir=logdir,
                               saver=saver,
                               global_step=global_step,
                               stop_grace_secs=300,
                               save_model_secs=0)
      output_dir = hdfs_path(args.output)
      output_file = tf.gfile.Open("{0}/part-{1:05d}".format(output_dir, task_index), mode='w')

    # The supervisor takes care of session initialization, restoring from
    # a checkpoint, and closing when done or an error occurs.
    with sv.managed_session(server.target) as sess:
      print("{0} session ready".format(datetime.now().isoformat()))

      # Loop until the supervisor shuts down or 1000000 steps have completed.
      step = 0
      count = 0
      while not sv.should_stop() and step < args.steps:
        # Run a training step asynchronously.
        # See `tf.train.SyncReplicasOptimizer` for additional details on how to
        # perform *synchronous* training.

        # using QueueRunners/Readers
        if args.mode == "train":
          if (step % 100 == 0):
            print(
              "{0} step: {1} accuracy: {2}".format(datetime.now().isoformat(), step, sess.run(accuracy)))
          _, summary, step = sess.run([train_op, summary_op, global_step])
          summary_writer.add_summary(summary, step)
        else:  # args.mode == "inference"
          labels, pred, acc = sess.run([label, prediction, accuracy])
          # print("label: {0}, pred: {1}".format(labels, pred))
          print("acc: {0}".format(acc))
          for i in range(len(labels)):
            count += 1
            output_file.write("{0} {1}\n".format(labels[i], pred[i]))
          print("count: {0}".format(count))

    if args.mode == "inference":
      output_file.close()
      # Delay chief worker from shutting down supervisor during inference, since it can load model, start session,
      # run inference and request stop before the other workers even start/sync their sessions.
      if task_index == 0:
        time.sleep(60)

    # Ask for all the services to stop.
    print("{0} stopping supervisor".format(datetime.now().isoformat()))
    sv.stop()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("-e", "--epochs", help="number of epochs", type=int, default=0)
  parser.add_argument("-f", "--format", help="example format: (csv|pickle|tfr)", choices=["csv", "pickle", "tfr"],
                      default="tfr")
  parser.add_argument("-i", "--images", help="HDFS path to MNIST images in parallelized format")
  parser.add_argument("-l", "--labels", help="HDFS path to MNIST labels in parallelized format")
  parser.add_argument("-m", "--model", help="HDFS path to save/load model during train/test", default="mnist_model")
  parser.add_argument("-o", "--output", help="HDFS path to save test/inference output", default="predictions")
  parser.add_argument("-r", "--readers", help="number of reader/enqueue threads", type=int, default=1)
  parser.add_argument("-s", "--steps", help="maximum number of steps", type=int, default=1000)
  parser.add_argument("-X", "--mode", help="train|inference", default="train")
  args = parser.parse_args()
  print("args:", args)

  print("{0} ===== Start".format(datetime.now().isoformat()))
  main(args)
  print("{0} ===== Stop".format(datetime.now().isoformat()))
