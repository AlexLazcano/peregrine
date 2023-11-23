#!/bin/bash
#
#SBATCH --cpus-per-task=1
#SBATCH --time=03:30
#SBATCH --mem=5G
#SBATCH --partition=slow
#SBATCH --ntasks=1


srun /home/$USER/master/bin/count ../data/citeseer 5-motifs 1
