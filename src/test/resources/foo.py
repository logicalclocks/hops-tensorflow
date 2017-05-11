from __future__ import print_function

import os

import bar

bar.hello_from_baz()

if 'YARNTF_PROTOCOL' in os.environ:
  print('YARNTF_PROTOCOL=' + os.environ['YARNTF_PROTOCOL'])
