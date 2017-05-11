from __future__ import print_function

import os
import sys
import yarntf

print('Number of arguments: ' + str(len(sys.argv)))
print('Argument list: ' + str(sys.argv))

cluster, server = yarntf.createClusterServer()

if 'YARNTF_TB_DIR' in os.environ:
  print('YARNTF_TB_DIR=' + os.environ['YARNTF_TB_DIR'])
