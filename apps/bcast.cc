#include <thread>
#include <chrono>

#include "mpi.h"
#include "Peregrine.hh"
#include "RangeQueue.hh"

int main(int argc, char const *argv[])
{
    int world_rank, world_size;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    printf("Hello world from process %d, of %d\n", world_rank, world_size);
    Peregrine::RangeQueue rq(world_rank, world_size);

    MPI_Barrier(MPI_COMM_WORLD);
    std::this_thread::sleep_for(std::chrono::seconds(world_rank));
    printf("RANK %d finished \n", world_rank);
    rq.broadcastFinished();

    while (true)
    {

        bool success = false;

        success = rq.handleBcasts();

        if (success == true)
        {
            break;
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    printf("DONE Process %d\n", world_rank);
    MPI_Finalize();
    return 0;
}
