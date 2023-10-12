FROM ubuntu:20.04
WORKDIR /peregrine
# needed to setup env properly, this disables user interaction which causes hangup on install otherwise
ENV DEBIAN_FRONTEND=noninteractive 
# Get dependencies
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
    cmake \
    g++ \
    g++-10 \
    gcc \
    libopenmpi-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy data
COPY . .

# run make
RUN make -j CC=g++-10

# run tests
CMD ["/peregrine/bin/test"]
