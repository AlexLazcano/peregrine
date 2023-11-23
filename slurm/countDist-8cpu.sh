#!/bin/bash
#
#SBATCH --cpus-per-task=8
#SBATCH --time=03:30
#SBATCH --mem=5G
#SBATCH --partition=slow
#SBATCH --ntasks=1


srun /home/$USER/peregrine/bin/countMPI ../data/citeseer 6-motifs 8
