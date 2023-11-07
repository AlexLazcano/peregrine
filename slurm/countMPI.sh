#!/bin/bash
#
#SBATCH --cpus-per-task=4
#SBATCH --time=00:30
#SBATCH --mem=5G
#SBATCH --partition=slow
#SBATCH --nodelist=cs-cloud-02

srun mpirun -np 4 /home/$USER/peregrine/bin/countMPI ../data/citeseer 6-motifs 1