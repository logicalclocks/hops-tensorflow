from __future__ import print_function

import os
import sys
import yarntf

print('Number of arguments: ' + str(len(sys.argv)))
print('Argument list: ' + str(sys.argv))

cluster, server = yarntf.createClusterServer()

if 'TB_DIR' in os.environ:
    print('TB_DIR=' + os.environ['TB_DIR'])
