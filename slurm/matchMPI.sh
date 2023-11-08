#!/bin/bash
#
#SBATCH --cpus-per-task=2
#SBATCH --time=00:30
#SBATCH --ntasks=2
#SBATCH --mem=5G
#SBATCH --partition=slow

srun /home/$USER/peregrine/bin/match