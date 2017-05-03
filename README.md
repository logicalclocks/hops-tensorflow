# Hops-TensorFlow
Easy distributed TensorFlow on Hops Hadoop. See our Python package [**yarntf**](yarntf/) repository for examples.

## Compatibility
Since GPU support was added, Hops-TensorFlow is not compatible with Apache Hadoop. If you want to use Hops-TensorFlow
 with Apache Hadoop, either use v0.0.1 or remove Hops dependencies from the code.

- Tested with Python 2.7
- yarntf examples adapted for TensorFlow v1.0.0+


## Before building
Install the Python package, run `pip install yarntf` on your machine and your cluster.

## How to build
With [**Hops Hadoop**](https://github.com/hopshadoop/hops) and its dependencies installed: `mvn clean install -Pndb`

## License
`Hops-TensorFlow` is released under an [Apache 2.0 license](LICENSE.txt).
