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

    std::vector<Peregrine::Range> work_done(0);

    auto do_work = [world_rank, &rq](std::vector<Peregrine::Range> *list)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        rq.setDoneRequesting(true);
        while (true)
        {
            auto maybeRange = rq.popRange();
            if (world_rank % 2 == 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds((world_rank+1) * 5));
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds((world_rank) * 50));
            }

            if (!maybeRange.has_value())
            {

                if (rq.done_stealing && rq.isQueueEmpty())
                {
                    break;
                }
                continue;
            }
            auto range = maybeRange.value();
            // printf("Rank %d popped %ld %ld\n", world_rank, range.first, range.second);
            list->emplace_back(range);
        }
    };

    rq.openSignal();
    rq.initRobbers();

    // Requesting Ranges
    int size = 100;
    Peregrine::Range range(world_rank * size, (world_rank + 1) * size);
    rq.split_addRange(range, size);
    // Processing current ranges ranges

    std::thread th(do_work, &work_done);

    while (true)
    {
        bool allProcessesDone = rq.handleSignal();
        if (allProcessesDone)
        {
            rq.done_stealing = true;
            break;
        }
        int hasRobber = rq.checkRobbers();
        if (hasRobber)
        {
            // printf("Rank %d has robber\n", world_rank);
            bool gotRobbed = rq.handleRobbers();
        }

        if (rq.done_ranges_given)
        {
            // printf("done ranges for current %d\n", world_rank);
            bool receivedRange = rq.stealRangeAsync();
            if (!receivedRange)
            {
                // printf("Rank %d Did not receive\n", world_rank);
            }
        }
        // rq.printActive();

        // std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    printf("Rank %d waiting for thread\n", world_rank);
    th.join();

    double count = 0.0;
    for (auto const &[first, second] : work_done)
    {
        count += 1.0;
        printf("Rank %d: %ld %ld \n", world_rank, first, second);
    }

    double percent = count / (size * world_size);

    MPI_Barrier(MPI_COMM_WORLD);
    printf("Work done by each process: %.1f / %d = %.1f%\n", count, size * world_size, percent * 100);

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
