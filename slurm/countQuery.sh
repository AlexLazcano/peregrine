#!/bin/bash
#
#SBATCH --cpus-per-task=1
#SBATCH --time=03:30
#SBATCH --mem=10G
#SBATCH --partition=slow
#SBATCH --ntasks=4


srun /home/$USER/peregrine/bin/countMPI ../data/citeseer ../query/test.graph 1
