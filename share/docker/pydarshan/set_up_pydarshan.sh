#!/bin/bash

# install requirements to build pydarshan
# moved to the dockerfile
#apt-get update
#apt-get install autoconf
#apt-get install libtool
#apt-get install libz-dev
#apt-get install python3-pip
#apt-get install python3.12-venv
#apt install python-is-python3

# use venv that was made in Dockerfile
source /pydarshan_venv/bin/activate

# clone repo; switch to experimental branch
cd /pydarshan_git
git clone https://github.com/darshan-hpc/darshan.git
cd darshan
git switch snyder/dxt-extra-info-pthread-id

# prepare for making
./prepare.sh
cd darshan-util
mkdir /darshan-util
./configure --prefix="/darshan-util"

# make
make
make install

# pip install; set library path so darshan knows where it is
cd /pydarshan_git/darshan/darshan-util/pydarshan
pip install .
export LD_LIBRARY_PATH="/darshan-util/lib:$LD_LIBRARY_PATH"
python -c 'import darshan' # test that it works
