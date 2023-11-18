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

    if (world_rank == 0)
    {
        for (uint i = 0; i < 10; i++)
        {
            auto range = Peregrine::Range(i, i + 1);
            rq.addRange(range);
        }
        rq.printRanges();
        rq.openSignal();
        
    bool active = true;
    start:
        rq.initRobbers();

        while (rq.handleSignal())
        {
            if (rq.isQueueEmpty() && active)
            {
                rq.signalDone();
                active = false;
            }
            
            printf("working\n");
            std::this_thread::sleep_for(std::chrono::seconds(1));
            printf("checking\n");
            int flag = rq.checkRobbers();
            if (flag)
            {
                printf("Robber detected\n");
                bool success = rq.handleRobbers();
                if (success)
                {
                    goto start;
                }
            }
        }
    }
    else
    {
        rq.signalDone();
        rq.handleSignal();
        bool running = true;
        while (running)
        {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            running = rq.stealRange();
        }
    }

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
