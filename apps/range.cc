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
            auto range = Peregrine::Range(i, i+1);
            rq.addRange(range);
        }
        rq.printRanges();
        MPI_Status status;
        MPI_Request req;
        uint64_t buffer[2];
        rq.initRobbers(req, buffer);
        

        while (true)
        {
            printf("working\n");
            std::this_thread::sleep_for(std::chrono::seconds(1));
            printf("checking\n");
            int flag = rq.checkRobbers(status, req);
            if (flag)
            {
                printf("done\n");
                break;
            }
            
            
        }
        

        
    } else {

        std::this_thread::sleep_for(std::chrono::seconds(3));

        rq.stealRange();
    }
    






    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
