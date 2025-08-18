#!/bin/bash -l
#PBS -l walltime=0:59:00
#PBS -q run_next
#PBS -A radix-io
#PBS -l filesystems=home:grand:eagle

set -eu 
cd $PBS_O_WORKDIR

ml load gcc/12.2.0 PrgEnv-gnu cudatoolkit-standalone libfabric/1.15.2.0 cray-mpich/8.1.25
source  ~/spack/share/spack/setup-env.sh
spack env activate mofkadask

spack find -fN

SSGFILE=mofka.ssg
SCHEFILE=scheduler.json
CONFIGFILE=config.json
DCONFIGFILE=config.txt
PROTOCOL=cxi
NDEPTH=32

export DXT_ENABLE_IO_TRACE=1
export DARSHAN_LOG_DIR_PATH=$PBS_O_WORKDIR
# export HG_LOG_LEVEL=debug

# Split nodes between the different steps
total=$(wc -l $PBS_NODEFILE | awk '{print $1}')

start_1=1
end_1=1

start_2=$(echo "${end_1}+1" | bc)
end_2=$(echo "${start_2}" | bc)

start_3=$(echo "${end_2}+1" | bc)
end_3=$(echo "${start_3}" | bc)

start_4=$(echo "${end_3}+1" | bc)
end_4=$(echo "${start_4}" | bc)

start_5=$(echo "${end_4}+1" | bc)
end_5=$total


sed -n "${start_1},${end_1}p" $PBS_NODEFILE > SchedulerNode
sed -n "${start_2},${end_2}p" $PBS_NODEFILE > ClientNode
sed -n "${start_3},${end_3}p" $PBS_NODEFILE > ConsumerNode
sed -n "${start_4},${end_4}p" $PBS_NODEFILE > MofkaServerNode
sed -n "${start_5},${end_5}p" $PBS_NODEFILE > WorkerNodes

echo Creating Mofka Server

mpiexec  -n 1 --ppn 1 -d ${NDEPTH} --hostfile MofkaServerNode bedrock $PROTOCOL -c $CONFIGFILE 1>>bedrock.o 2>>bedrock.e &

# Wait for the SSGFILE to be created
while ! [ -f $SSGFILE ]; do
    sleep 1
    echo -n .
done


echo launching Scheduler
mpiexec  -n 1 --ppn 1 -d ${NDEPTH} --hostfile SchedulerNode --exclusive  --cpu-bind depth  dask scheduler --scheduler-file=$SCHEFILE  --preload MofkaSchedulerPlugin.py  --mofka-protocol=$PROTOCOL  --ssg-file=$SSGFILE 1>> scheduler.o  2>> scheduler.e  &
bedrock_pdi=$!

# Wait for the SCHEFILE to be created
while ! [ -f $SCHEFILE ]; do
    sleep 1
    echo -n .
done

# Connect the client to the Dask scheduler
echo Connect Master Client

#mpiexec  -n 1 --ppn 1  -d ${NDEPTH} --hostfile ClientNode  --exclusive  --cpu-bind depth  `which python` producer.py  --scheduler-file=$SCHEFILE  1>> producer.o 2>> producer.e &

mpiexec  -n 1 --ppn 1  -d ${NDEPTH} --hostfile ClientNode  --exclusive  --cpu-bind depth  `which python` image_processing.py --mode=distributed --scheduler-file=$SCHEFILE  1>> producer.o 2>> producer.e &
client_pid=$!

# Launch Dask workers in the rest of the allocated nodes
echo Scheduler booted, Client connected, launching workers

while read node; do
	DARSHAN_ENABLE_NONMPI=1 DARSHAN_CONFIG_PATH="config.txt" LD_PRELOAD="/home/agueroudji/spack/opt/spack/linux-sles15-zen3/gcc-11.2.0/darshan-runtime-dask-a3mpgplad6blsmn4vgvsce6mexozglja/lib/libdarshan.so" mpiexec -n 4 --ppn 4  -d 8 --hosts $node --exclusive --cpu-bind depth dask worker --scheduler-file=$SCHEFILE --preload MofkaWorkerPlugin.py  --mofka-protocol=$PROTOCOL  --ssg-file=$SSGFILE 1>> worker.o  2>> worker.e  &
done < WorkerNodes

# Connect the Mofka consumer client
echo Connect Mofka consumer client
mpiexec  -n 1 --ppn 1  -d ${NDEPTH} --hostfile ConsumerNode --exclusive --cpu-bind depth  `which python` consumer.py --mofka-protocol=$PROTOCOL  --ssg-file=$SSGFILE 1>> consumer.o 2>> consumer.e 
consumer_pid=$!

#echo Stopping bedrock
mpiexec -n 1 --ppn 1 bedrock-shutdown $PROTOCOL -s $SSGFILE 1> bedrock-shutdown.out 2> bedrock-shutdown.err 
rm -rf __pycache__ 2024*

# Wait for the client process and Mofka consumer to be finished
wait $client_pid
wait $consumer_pid
wait $bedrock_pdi

