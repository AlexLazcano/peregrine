#include <iostream>
#include <mpi.h>

int main(int argc, char **argv)
{
    printf("Hello world\n");

    int process_Rank, size_cluster;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size_cluster);
    MPI_Comm_rank(MPI_COMM_WORLD, &process_Rank);

    printf("Hello world from process %d, of %d\n", process_Rank, size_cluster);

    MPI_Finalize();

    return 0;
}
