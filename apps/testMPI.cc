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
        Peregrine::VertexQueue queue(1000, world_size-1);
                queue.coordinate();
    }
    else
    {
        printf("Rank %d starting request\n", world_rank);
        bool successful = true;
        std::pair<uint64_t, uint64_t> result;
        int count = 0;
        std::vector<std::pair<uint64_t, uint64_t>> ranges_vector;

        while (true)
        {

            successful = Peregrine::request_range(result, world_rank, count);

            count++;
            if (!successful)
            {
                // printf("unsuccessful done processing %d\n", world_rank);
                break;
            }
            else
            {
                // printf("%ld %ld \n", result.first, result.second);
                ranges_vector.emplace_back(result);
            }
        }

        for (auto range : ranges_vector)
        {
            printf("%d: %ld %ld\n", world_rank, range.first, range.second);
        }
    }

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
