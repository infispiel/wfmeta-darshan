2024-04-18 15:54:27,901 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:54:28,108 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:54:28,109 - distributed.scheduler - INFO - -----------------------------------------------
2024-04-18 15:54:28,112 - distributed.preloading - INFO - Creating preload: MofkaSchedulerPlugin.py
2024-04-18 15:54:28,113 - distributed.utils - INFO - Reload module MofkaSchedulerPlugin from .py file
2024-04-18 15:54:28,119 - distributed.preloading - INFO - Import preload module: MofkaSchedulerPlugin.py
2024-04-18 15:54:29,124 - distributed.http.proxy - INFO - To route to workers diagnostics web server please install jupyter-server-proxy: python -m pip install jupyter-server-proxy
2024-04-18 15:54:29,164 - distributed.scheduler - INFO - State start
2024-04-18 15:54:29,168 - distributed.scheduler - INFO - -----------------------------------------------
/home/agueroudji/spack/var/spack/environments/mofkadask/.spack-env/view/lib/python3.10/site-packages/distributed/utils.py:181: RuntimeWarning: Couldn't detect a suitable IP address for reaching '8.8.8.8', defaulting to hostname: [Errno 101] Network is unreachable
  warnings.warn(
2024-04-18 15:54:29,186 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.201.0.220:8786
2024-04-18 15:54:29,187 - distributed.scheduler - INFO -   dashboard at:  http://10.201.0.220:8787/status
2024-04-18 15:54:29,189 - distributed.preloading - INFO - Run preload setup: MofkaSchedulerPlugin.py
2024-04-18 15:54:30,090 - distributed.scheduler - INFO - Registering Worker plugin shuffle
2024-04-18 15:54:34,043 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:38553', status: init, memory: 0, processing: 0>
2024-04-18 15:54:34,577 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:38553
2024-04-18 15:54:34,577 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:44442
2024-04-18 15:54:34,579 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:34699', status: init, memory: 0, processing: 0>
2024-04-18 15:54:34,580 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:34699
2024-04-18 15:54:34,580 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:44454
2024-04-18 15:54:34,582 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:45449', status: init, memory: 0, processing: 0>
2024-04-18 15:54:34,583 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:45449
2024-04-18 15:54:34,583 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:44426
2024-04-18 15:54:34,584 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.212:38577', status: init, memory: 0, processing: 0>
2024-04-18 15:54:34,584 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.212:38577
2024-04-18 15:54:34,584 - distributed.core - INFO - Starting established connection to tcp://10.201.0.212:44436
2024-04-18 15:54:35,426 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:37485', status: init, memory: 0, processing: 0>
2024-04-18 15:54:35,426 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:37485
2024-04-18 15:54:35,427 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:37194
2024-04-18 15:54:35,428 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:41793', status: init, memory: 0, processing: 0>
2024-04-18 15:54:35,428 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:41793
2024-04-18 15:54:35,428 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:37176
2024-04-18 15:54:35,429 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:44955', status: init, memory: 0, processing: 0>
2024-04-18 15:54:35,429 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:44955
2024-04-18 15:54:35,429 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:37184
2024-04-18 15:54:35,430 - distributed.scheduler - INFO - Register worker <WorkerState 'tcp://10.201.0.229:45945', status: init, memory: 0, processing: 0>
2024-04-18 15:54:35,430 - distributed.scheduler - INFO - Starting worker compute stream, tcp://10.201.0.229:45945
2024-04-18 15:54:35,430 - distributed.core - INFO - Starting established connection to tcp://10.201.0.229:37200
2024-04-18 15:54:39,708 - distributed.scheduler - INFO - Receive client connection: Client-f6c10846-fd9b-11ee-a738-6805cae12036
2024-04-18 15:54:39,709 - distributed.core - INFO - Starting established connection to tcp://10.201.0.223:50244
2024-04-18 15:55:32,887 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 8.39s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:56:36,148 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.20s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:56:47,548 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 11.31s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:57:59,790 - distributed.core - INFO - Event loop was unresponsive in Scheduler for 9.22s.  This is often caused by long-running GIL-holding functions or moving large chunks of data. This can cause timeouts and instability.
2024-04-18 15:57:59,803 - distributed.scheduler - INFO - Scheduler closing due to unknown reason...
2024-04-18 15:57:59,804 - distributed.scheduler - INFO - Scheduler closing all comms
2024-04-18 15:57:59,806 - distributed.core - INFO - Connection to tcp://10.201.0.212:44442 has been closed.
2024-04-18 15:57:59,806 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:38553', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455879.806289')
2024-04-18 15:58:00,361 - distributed.core - INFO - Connection to tcp://10.201.0.212:44454 has been closed.
2024-04-18 15:58:00,361 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:34699', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455880.3618543')
2024-04-18 15:58:00,362 - distributed.core - INFO - Connection to tcp://10.201.0.212:44426 has been closed.
2024-04-18 15:58:00,362 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:45449', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455880.3624494')
2024-04-18 15:58:00,363 - distributed.core - INFO - Connection to tcp://10.201.0.212:44436 has been closed.
2024-04-18 15:58:00,363 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.212:38577', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455880.3631325')
2024-04-18 15:58:00,363 - distributed.core - INFO - Connection to tcp://10.201.0.229:37194 has been closed.
2024-04-18 15:58:00,363 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:37485', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455880.3637393')
2024-04-18 15:58:00,364 - distributed.core - INFO - Connection to tcp://10.201.0.229:37176 has been closed.
2024-04-18 15:58:00,364 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:41793', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455880.3643682')
2024-04-18 15:58:00,364 - distributed.core - INFO - Connection to tcp://10.201.0.229:37184 has been closed.
2024-04-18 15:58:00,364 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:44955', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455880.364883')
2024-04-18 15:58:00,365 - distributed.core - INFO - Connection to tcp://10.201.0.229:37200 has been closed.
2024-04-18 15:58:00,365 - distributed.scheduler - INFO - Remove worker <WorkerState 'tcp://10.201.0.229:45945', status: running, memory: 0, processing: 0> (stimulus_id='handle-worker-cleanup-1713455880.3654556')
2024-04-18 15:58:00,365 - distributed.scheduler - INFO - Lost all workers
2024-04-18 15:58:00,368 - distributed.scheduler - INFO - Stopped scheduler at 'tcp://10.201.0.220:8786'
2024-04-18 15:58:00,368 - distributed.scheduler - INFO - End scheduler
