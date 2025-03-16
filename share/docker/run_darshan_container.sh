#! /bin/bash

# make sure a folder is provided to mount
if [ "$#" -ne 2 ]; then
	echo "Usage: $0 <code folder to mount> <data folder to mount>"
	exit 1
fi

# turn relative path into absolute
function to-abs-path {
    local target="$1"

    if [ "$target" == "." ]; then
        echo "$(pwd)"
    elif [ "$target" == ".." ]; then
        echo "$(dirname "$(pwd)")"
    else
        echo "$(cd "$(dirname "$1")"; pwd)/$(basename "$1")"
    fi
}

codepath=$(to-abs-path $1)
datapath=$(to-abs-path $2)
#customrepopath=$(to-abs-path ~/spack_custom_repo)

docker run \
	-v $codepath:/code \
	-v $datapath:/data \
	--interactive \
	--tty \
	spack_autodef:dev
