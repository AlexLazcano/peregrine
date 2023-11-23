#!/bin/bash
#
# Generate a bash script with multiple Slurm job submissions

# Specify the output script file
directory="countMPI"

if [ ! -d "$directory" ]; then
    mkdir "$directory"
    echo "Directory '$directory' created."
else
    echo "Directory '$directory' already exists."
fi



# Function to generate a job submission command
generate_job_command() {
    local data_dir=$1
    local motif_type=$2
    local cpus_per_task=$3
    local time_limit=$4
    local mem_limit=$5
    local n_tasks=$6
    local partition=$7


    echo "#!/bin/bash
#
#SBATCH --cpus-per-task=$cpus_per_task 
#SBATCH --time=$time_limit
#SBATCH --mem=10G
#SBATCH --partition=$partition
#SBATCH --ntasks=$n_tasks
#SBATCH --output=$motif_type.$n_tasks.$cpus_per_task.$partition.txt \n
srun  /home/alazcano/peregrine/bin/countMPI $data_dir $motif_type $cpus_per_task
"

}

# Specify different parameter sets
data_dirs=("/home/alazcano/peregrine/data/citeseer")
pattern=("5-motifs" "6-motifs")
total_cpus=(1 2 4 8)
partitions=("slow" "fast")
# cpus_per_task=1
time_limit="03:30"
mem_limit="5G"
n_tasks_values=(1 2 4)


# Generate the script content
# script_content="#!/bin/bash\n\n"

# Loop over parameter sets and generate job commands
for data_dir in "${data_dirs[@]}"; do
    for motif_type in "${pattern[@]}"; do
        for n_tasks in "${n_tasks_values[@]}"; do
            for cpu_count in "${total_cpus[@]}"; do
                for partition in "${partitions[@]}"; do
                    cpus_per_task=$((cpu_count / n_tasks))
                    cpus_per_task=$((cpus_per_task < 1 ? 1 : cpus_per_task))
                    script_content="$(generate_job_command "$data_dir" "$motif_type" $cpus_per_task $time_limit $mem_limit $n_tasks $partition)\n\n"
                    # Write the script content to the output script file
                    output_script="./$directory/$motif_type.$n_tasks.$cpus_per_task.$partition.sh"
                    echo -e "$script_content" > $output_script

                    # Make the script executable
                    chmod +x "$output_script"

                    echo "Job submission script generated: $output_script"
                done
            done
        done
    done
done

# # Write the script content to the output script file
# echo -e "$script_content" > "$output_script"

# # Make the script executable
# chmod +x "$output_script"

# echo "Job submission script generated: $output_script"