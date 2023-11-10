#!/bin/bash


# Run the MPI application
mpirun -np 2 --hostfile /peregrine/hostfile /peregrine/bin/countMPI /peregrine/data/citeseer 4-motifs 1
