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

    auto do_work = [world_rank, &rq]()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds((world_rank+1)*500));
        rq.signalDone();
        printf("RANK %d Signaled\n", world_rank);
    };

    rq.openSignal();

    if (world_rank == 0)
    {
        Peregrine::Range range(0, 1000);
        rq.split_addRange(range, 100);

    }
    else
    {
        Peregrine::Range range(0, 100);
        rq.split_addRange(range, 10);
    }

    std::thread th(do_work);

    while (true)
    {
        bool finished = rq.handleSignal();
        if (finished)
        {
            break;;
        }
        
    }
    th.join();

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
