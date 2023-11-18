#!/bin/bash
#
#SBATCH --cpus-per-task=8
#SBATCH --time=03:30
#SBATCH --mem=5G
#SBATCH --partition=fast
#SBATCH --ntasks=1

NUM=7

srun /home/$USER/master/bin/count ../data/citeseer $NUM-motifs 6
