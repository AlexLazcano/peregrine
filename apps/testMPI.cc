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

    // Peregrine::VertexQueue queue(100, 1);
    // queue.step = 7;
    // std::pair<uint64_t, uint64_t> range;

    // while (queue.curr < queue.end)
    // {
    //     bool succ = queue.get_v_range(range);
    //     printf("%ld %ld\n", range.first, range.second);
    // }

    std::thread coordThread;

    if (world_rank == 0)
    {
        coordThread = std::thread(
            [world_size]()
            {
                Peregrine::VertexQueue queue(10, world_size);
                queue.coordinate();
            });
    }
    printf("Rank %d starting request\n", world_rank);
    bool successful = true;
    std::pair<uint64_t, uint64_t> result;
    int count = 0;

    while (true)
    {
        successful = Peregrine::request_range(result, world_rank, count);
        count++;
        if (!successful)
        {
            printf("unsuccessful done processing %d\n", world_rank);
            break;
        }
    }

    if (world_rank == 0)
    {
        printf("joining \n");
        coordThread.join();
    }

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
