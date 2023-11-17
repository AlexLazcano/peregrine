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

    printf("DONE Process %d\n", world_rank);
    MPI_Finalize();
    return 0;
}
