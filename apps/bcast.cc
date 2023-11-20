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
    if (world_rank == world_size - 1)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    printf("RANK %d finished \n", world_rank);
    rq.openSignal();
    rq.signalDone();
    printf("signaled%d\n", world_rank);

    while (true)
    {

        bool success = false;

        success = rq.handleSignal();
        if (success)
        {
            break;
        }
        rq.showActive();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    printf("RANK %d waiting\n", world_rank);
    MPI_Barrier(MPI_COMM_WORLD);
    rq.showActive();
    rq.waitAllSends();

    printf("DONE Process %d\n", world_rank);
    MPI_Finalize();
    return 0;
}
