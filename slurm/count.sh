#!/bin/bash
#
#SBATCH --cpus-per-task=2
#SBATCH --time=03:30
#SBATCH --mem=5G
#SBATCH --partition=fast
#SBATCH --ntasks=3
#SBATCH --nodelist=cs-cloud-04



srun /home/$USER/master/bin/count ../data/citeseer 6-motifs 1
srun /home/$USER/master/bin/count ../data/citeseer 6-motifs 2
srun /home/$USER/peregrine/bin/countMPI ../data/citeseer 6-motifs 1
srun /home/$USER/peregrine/bin/countMPI ../data/citeseer 6-motifs 2
# srun /home/$USER/peregrine/bin/countMPI ../data/citeseer 6-motifs 3
