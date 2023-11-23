
#!/bin/bash
#
#SBATCH --cpus-per-task=1 
#SBATCH --time=03:30
#SBATCH --mem=10G
#SBATCH --partition=slow
#SBATCH --n_tasks=4
srun /home/alazcano/peregrine/bin/countMPI ../data/citeseer 5-motifs 1


