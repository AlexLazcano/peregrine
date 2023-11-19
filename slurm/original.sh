#!/bin/bash
#
#SBATCH --cpus-per-task=8
#SBATCH --time=03:30
#SBATCH --mem=5G
#SBATCH --partition=fast
#SBATCH --ntasks=1
#SBATCH --nodelist=cs-cloud-04


srun /home/$USER/master/bin/count ../data/citeseer 6-motifs 8
