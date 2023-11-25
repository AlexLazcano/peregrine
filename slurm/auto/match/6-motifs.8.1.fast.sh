#!/bin/bash
#
#SBATCH --cpus-per-task=1 
#SBATCH --time=01:30
#SBATCH --mem=10G
#SBATCH --partition=fast
#SBATCH --ntasks=8
#SBATCH --output=6-motifs.8.1.fast.txt 

srun  /home/alazcano/peregrine/bin/match /home/alazcano/peregrine/data/citeseer 6-motifs 1


