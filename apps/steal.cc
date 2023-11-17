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

    int end = world_rank*2 + 2;
    for (int i = 0; i < end; i++)
    {
        int s = (world_rank*10) +i;
        rq.addRange(Peregrine::Range(s, s+1));
    }

    rq.printRanges();

    while (true)
    {
        auto maybeRange = rq.popFirstRange();

        if (!maybeRange.has_value())
        {
            break;
        }
        auto range = maybeRange.value();
        
    }
    



    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
