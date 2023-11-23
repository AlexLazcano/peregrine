#!/bin/bash
#
# Submit multiple jobs to Slurm with different parameters

# Function to submit a job
submit_job() {
    local data_dir=$1
    local motif_type=$2
    local cpus_per_task=$3
    local time_limit=$4
    local mem_limit=$5
    local n_tasks=$6


    sbatch --cpus-per-task=$cpus_per_task \
           --time=$time_limit \
           --mem=$mem_limit \
           --partition=slow \
           --ntasks=$n_tasks \
           --output=output_${motif_type}.txt \
           --error=error_${motif_type}.txt \
           /home/alazcano/peregrine/bin/countMPI $data_dir $motif_type $cpus_per_task
}

# Specify different parameter sets
data_dirs=("../data/citeseer")
pattern=("5-motifs" "6-motifs")
cpus_per_task=8
time_limit="03:30"
mem_limit="5G"
n_tasks_values=(1 2 4)
n_threads=2

# Loop over parameter sets and submit jobs
for data_dir in "${data_dirs[@]}"; do
    for motif_type in "${pattern[@]}"; do
        submit_job "$data_dir" "$motif_type" $cpus_per_task $time_limit $mem_limit 
    done
done
