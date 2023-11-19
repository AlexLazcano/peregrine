#!/bin/bash
#
#SBATCH --cpus-per-task=1
#SBATCH --time=03:30
#SBATCH --mem=5G
#SBATCH --partition=fast
#SBATCH --ntasks=8
#SBATCH --nodelist=cs-cloud-04



srun /home/$USER/peregrine/bin/countMPI ../data/citeseer 6-motifs 1
