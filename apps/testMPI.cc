#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "mpi.h"
#include <thread>
#include "Peregrine.hh"

bool is_directory(const std::string &path)
{
    struct stat statbuf;
    if (stat(path.c_str(), &statbuf) != 0)
        return 0;
    return S_ISDIR(statbuf.st_mode);
}

int main(int argc, char *argv[])
{

    int world_rank, world_size;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    printf("Hello world from process %d, of %d\n", world_rank, world_size);



    if (world_rank == 0)
    {
        Peregrine::VertexQueue queue(100, world_size -1);
        std::jthread thread([&queue]() {
            queue.coordinate();
        });
    }
    else
    {
        bool successful = true;
        std::pair<uint64_t, uint64_t> result;
        while (successful)
        {
            printf("Rank %d requesting\n", world_rank);
            successful = Peregrine::request_range(&result);
            printf("Rank %d recevied %ld %ld\n", world_rank, result.first, result.second);
        }
    }

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
