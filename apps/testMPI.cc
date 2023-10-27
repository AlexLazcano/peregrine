#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include "mpi.h"
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
        auto t1 = utils::get_timestamp();
        queue.coordinate();
        auto t2 = utils::get_timestamp();
        utils::Log{} << "distribution took: " << (t2-t1)/1e6 << "s" << "\n";
    }
    else
    {
        // printf("Rank %d starting request\n", world_rank);
        bool successful = true;
        std::pair<uint64_t, uint64_t> result;
        int count = 0;
        std::vector<std::pair<uint64_t, uint64_t>> ranges_vector;
        std::chrono::milliseconds duration(100);
        while (true)
        {

            successful = Peregrine::request_range(result);

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
            std::this_thread::sleep_for(duration);
        }

        // for (auto range : ranges_vector)
        // {
        //     printf("%d: %ld %ld\n", world_rank, range.first, range.second);
        // }
    }

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
