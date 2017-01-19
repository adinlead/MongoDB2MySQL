from time import sleep

import threadpool
from threadpool import ThreadPool

def test(idx,arg1,arg2):
    print '%s >>> %s' % (idx,arg1)
    print '%s >>> %s' % (idx,arg2)
    sleep(1)

pool = ThreadPool(2)

requests = []
requests.append(threadpool.WorkRequest(test,(1,0,1)))
requests.append(threadpool.WorkRequest(test,(2,2,3)))
requests.append(threadpool.WorkRequest(test,(3,4,5)))
[pool.putRequest(req) for req in requests]
pool.wait()