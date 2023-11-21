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

    Peregrine::Range full_range(0,3000);
    auto t1 = utils::get_timestamp();
    rq.coordinateScatter(full_range);
    auto t2 = utils::get_timestamp();
    utils::Log{} << "Work Distribution Time: " << (t2-t1) / 1e6 << "s" << "\n";

    printf("DONE Process %d\n", world_rank);
    MPI_Finalize();
    return 0;
}
