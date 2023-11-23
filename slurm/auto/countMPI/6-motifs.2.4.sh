
#!/bin/bash
#
#SBATCH --cpus-per-task=4 
#SBATCH --time=03:30
#SBATCH --mem=10G
#SBATCH --partition=slow
#SBATCH --n_tasks=2
srun /home/alazcano/peregrine/bin/countMPI ../data/citeseer 6-motifs 4


