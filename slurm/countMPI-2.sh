#!/bin/bash
#
#SBATCH --cpus-per-task=2
#SBATCH --time=00:30
#SBATCH --mem=5G
#SBATCH --partition=slow
#SBATCH --ntasks=3

srun /home/$USER/peregrine/bin/countMPI ../data/citeseer 4-motifs 2