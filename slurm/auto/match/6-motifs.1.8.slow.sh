#!/bin/bash
#
#SBATCH --cpus-per-task=8 
#SBATCH --time=01:30
#SBATCH --mem=10G
#SBATCH --partition=slow
#SBATCH --ntasks=1
#SBATCH --output=6-motifs.1.8.slow.txt 

srun  /home/alazcano/peregrine/bin/match /home/alazcano/peregrine/data/citeseer 6-motifs 8


