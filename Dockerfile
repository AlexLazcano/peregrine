FROM ubuntu:20.04
WORKDIR /peregrine
# needed to setup env properly, this disables user interaction which causes hangup on install otherwise
ENV DEBIAN_FRONTEND=noninteractive 
ENV C_INCLUDE_PATH=/usr/lib/x86_64-linux-gnu/openmpi/include/openmpi:/usr/lib/x86_64-linux-gnu/openmpi/include:$C_INCLUDE_PATH
ENV CXX=g++-10
# Get dependencies
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    cmake \
    g++ \
    g++-10 \
    gcc \
    libunittest++-dev\
    libmpich-dev \
    mpich\
    make \
    mpi-default-dev\
    libopenmpi-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*



# Copy data
COPY . .

RUN update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-10 30
RUN update-alternatives --config g++
# run make
RUN make countMPI

# run tests
CMD ["/bin/bash"]
