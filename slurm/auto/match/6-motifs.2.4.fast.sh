#!/bin/bash
#
#SBATCH --cpus-per-task=4 
#SBATCH --time=01:30
#SBATCH --mem=10G
#SBATCH --partition=fast
#SBATCH --ntasks=2
#SBATCH --output=6-motifs.2.4.fast.txt 

srun  /home/alazcano/peregrine/bin/match /home/alazcano/peregrine/data/citeseer 6-motifs 4


