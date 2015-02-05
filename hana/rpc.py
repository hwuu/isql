#
# Test remote invoking.
#
# Hao, created: 01/29/2015, modified: 01/29/2015
#

import os
import sys
from subprocess import Popen, PIPE

if __name__ == "__main__":
    cmd = "sudo ssh -i jzhou_10_128_84_28 jzhou@10.128.84.28 " + \
          "\"cd /home/jzhou/isql/; python rpc_server.py \"" + sys.argv[1]
    os.system(cmd)
    #p = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
    #out, err = p.communicate()
    #print "Return code: ", p.returncode
    #print out.rstrip(), err.rstrip()


