import os
import threading
import time

import yarntf

threads = []

os.environ['TENSORBOARD'] = 'true'
for i in range(0, 3):
    os.environ['TB_DIR'] = 'tensorboard_' + str(i)
    thread = threading.Thread(target=yarntf.createClusterSpec,
                              args=('localhost:50052', '(appId)', 'worker', i))
    thread.start()
    threads.append(thread)
    time.sleep(2)

for thread in threads:
    thread.join()
