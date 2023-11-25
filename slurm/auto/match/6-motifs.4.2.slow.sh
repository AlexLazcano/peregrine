#!/bin/bash
#
#SBATCH --cpus-per-task=2 
#SBATCH --time=01:30
#SBATCH --mem=10G
#SBATCH --partition=slow
#SBATCH --ntasks=4
#SBATCH --output=6-motifs.4.2.slow.txt 

srun  /home/alazcano/peregrine/bin/match /home/alazcano/peregrine/data/citeseer 6-motifs 2


