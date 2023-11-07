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
       
        for (size_t i = 0; i < 10; i++)
        {
            Peregrine::Range r(i, i+1);
            rq.addRange(r);
        }
        rq.printRanges();
        
        
    } else { 

        std::this_thread::sleep_for(std::chrono::seconds(3));

        rq.stealRange();

    }
    






    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
