from __future__ import print_function

import sys
import yarntf

cluster, server = yarntf.createClusterServer()

print('Number of arguments: ' + str(len(sys.argv)))
print('Argument list: ' + str(sys.argv))
