
#!/bin/bash
#
#SBATCH --cpus-per-task=4 
#SBATCH --time=03:30
#SBATCH --mem=10G
#SBATCH --partition=slow
#SBATCH --n_tasks=1
srun /home/alazcano/peregrine/bin/countMPI ../data/citeseer 5-motifs 4


