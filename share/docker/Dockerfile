# Build stage with Spack pre-installed and ready to be used
FROM spack/ubuntu-jammy:develop AS builder

ADD darshan /darshan-repo

# What we want to install and how we want to install it
# is specified in a manifest file (spack.yaml)
RUN mkdir -p /opt/spack-environment && \
set -o noclobber \
&&  (echo spack: \
&&   echo '  repos:' \
&&   echo '  - /darshan-repo' \
&&   echo '  specs:' \
&&   echo '  - darshan-util@dxt-pthread' \
&&   echo '  concretizer:' \
&&   echo '    unify: true' \
&&   echo '  view: /opt/views/view' \
&&   echo '  config:' \
&&   echo '    install_tree: /opt/software') > /opt/spack-environment/spack.yaml

# Install the software, remove unnecessary deps
RUN cd /opt/spack-environment && \
	spack env activate . && spack install --fail-fast && spack gc -y

# Strip all the binaries
RUN find -L /opt/views/view/* -type f -exec readlink -f '{}' \; | \
    xargs file -i | \
    grep 'charset=binary' | \
    grep 'x-executable\|x-archive\|x-sharedlib' | \
    awk -F: '{print $1}' | xargs strip

# Modifications to the environment that are necessary to run
RUN cd /opt/spack-environment && \
    spack env activate --sh -d . > activate.sh


# Bare OS image to run the installed executables
FROM ubuntu:22.04 AS core

COPY --from=builder /opt/spack-environment /opt/spack-environment
COPY --from=builder /opt/software /opt/software

# paths.view is a symlink, so copy the parent to avoid dereferencing and duplicating it
COPY --from=builder /opt/views /opt/views

SHELL ["/bin/bash", "-c"]

ADD pydarshan /pydarshan/
RUN mkdir /pydarshan_git
WORKDIR /pydarshan_git

RUN apt-get -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false update && \
    apt-get -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false install -y \
    autoconf \
    libtool \
    libz-dev \
    python3-pip \
    python3-venv \
    python-is-python3 \
    git

RUN python -m venv /pydarshan_venv && source /pydarshan_venv/bin/activate && \
    pip install ipykernel pyarrow && \
    chmod a+x /pydarshan/set_up_pydarshan.sh && /pydarshan/set_up_pydarshan.sh

RUN { \
      echo '#!/bin/bash' \
      && echo '.' /opt/spack-environment/activate.sh \
      && echo 'source /pydarshan_venv/bin/activate' \
      && echo 'exec "$@"'; \
    } > /entrypoint.sh \
&& chmod a+x /entrypoint.sh \
&& ln -s /opt/views/view /opt/view

ENTRYPOINT [ "/entrypoint.sh" ]
CMD [ "/bin/bash" ]

FROM core

RUN apt-get -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false update && \
    apt-get -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false install -y \
    vim && \
    source /pydarshan_venv/bin/activate && \
    pip install ipykernel && \
    pip install jupyter -U

EXPOSE 8888

RUN mkdir /resource/

RUN { \
      echo '#!/bin/bash' \
      && echo '.' /opt/spack-environment/activate.sh \
      && echo 'source /pydarshan_venv/bin/activate' \
      && echo 'jupyter notebook --ip=0.0.0.0 --allow-root --no-browser /resource/'; \
    } > /entrypoint_jupyter.sh \
&& chmod a+x /entrypoint_jupyter.sh \
&& ln -s /opt/views/view /opt/view

ENTRYPOINT ["/entrypoint_jupyter.sh"]
