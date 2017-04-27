from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
  long_description = f.read()

setup(
  name='yarntf',
  version='0.0.2.dev10',
  description='Easy distributed TensorFlow on Hops Hadoop',
  long_description=long_description,
  url='https://github.com/hopshadoop/hops-tensorflow',
  author='Tobias Johansson',
  author_email='tobias@johansson.xyz',
  license='Apache License 2.0',
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Libraries',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 2.7',
  ],
  keywords='yarn tf hops hadoop tensorflow',
  packages=find_packages(exclude=['examples', 'tests']),
  install_requires=['grpcio', 'tensorflow'],
)
