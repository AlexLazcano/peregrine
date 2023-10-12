#!/bin/sh
echo "MPI VERSION IS: "
mpicc --showme:version
source tbb2020/bin/tbbvars.sh intel64
make -j CC=g++-10
bin/test