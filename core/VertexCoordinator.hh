#ifndef VERTEX_COORDINATOR_HH
#define VERTEX_COORDINATOR_HH

#include "mpi.h"
namespace Peregrine
{
    static bool request_range(std::pair<uint64_t, uint64_t> &result)
    {

        MPI_Status status;
        int count;
        uint64_t buffer[2];
        // Tag 0 - Sending empty message to 0
        MPI_Send(buffer, 1, MPI_UINT64_T, 0, 0, MPI_COMM_WORLD);
        MPI_Probe(0, 1, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_UINT64_T, &count);
        if (count == 1)
        {
            // Tag 1 - Returns false since could not get any more ranges
            MPI_Recv(buffer, 1, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            return false;
        }
        else
        {
            // Tag 1 - Got more ranges
            MPI_Recv(buffer, 2, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            result = std::make_pair(buffer[0], buffer[1]);
            return true;
        }
    }

    class VertexCoordinator
    {
    private:
        uint64_t step;
        int nWorkers;
        std::atomic<uint64_t> curr = 0;
        uint64_t number_tasks = 0;
        int number_of_consumers;
        std::atomic<int> processesFinished = 0;
        std::atomic<int> threadsFinished = 0;
        bool finished = false;
        void coordinateWorker(int id, Barrier &b)
        {
            // printf("coordinating\n");
            MPI_Status status;
            // std::vector<uint64_t> buffer(2);
            uint64_t buffer[2];
            
            // printf("number of conusumer: %d tasks %ld\n", this->number_of_consumers, this->number_tasks);
            while (true)
            {
                auto maybeRange = this->get_v_range();

                if (!maybeRange.has_value())
                {
                    break;
                }
                
                // printf("recv %d\n", id);
                // Tag 0 - Receive empty message and see who its from
                MPI_Recv(buffer, 1, MPI_UINT64_T, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

                Range range = maybeRange.value();
                // printf("%d range: %ld %ld \n", id, range.first, range.second);
                buffer[0] = range.first;
                buffer[1] = range.second;
                // Tag 1 - size 2
                MPI_Send(buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
               
            }

            b.hit();

            if (id == 0)
            {
                int processesFinished = 0;

                while (processesFinished < this->number_of_consumers)
                {
                    // Tag 0 - Receive empty message and see who its from
                    MPI_Recv(buffer, 1, MPI_UINT64_T, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
                    // Tag 1 - size 1
                    MPI_Send(buffer, 1, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                    processesFinished++;
                }
            }
        }

    public:
        void reset()
        {
            this->curr = 0;
            this->processesFinished = 0;
        }

        void update_number_tasks(uint64_t numberTasks)
        {
            this->number_tasks = numberTasks;
        }
        void update_step(u_int64_t new_step) {
            this->step = new_step;
        }
        std::optional<Range> get_v_range()
        {
            uint64_t before = this->curr.fetch_add(this->step);
            if (before >= this->number_tasks)
            {
                return std::nullopt;
            }
            uint64_t last;

            if (before + this->step < this->number_tasks)
            {
                last = before + this->step;
            }
            else
            {
                last = this->number_tasks+1;
            }

            // range.first = before;
            // range.second = last;
            // this->curr += this->step;
            return Range(before, last);
        }

        VertexCoordinator(int numConsumers) : number_of_consumers(numConsumers)
        {
            this->step = 100;
        }
        VertexCoordinator(int numConsumers, int64_t steps_init, int nworkers)
        {
            this->number_of_consumers = numConsumers;
            this->step = steps_init;
            this->nWorkers = nworkers;
        }

        utils::timestamp_t coordinate()
        {
            std::vector<std::thread> pool;
            Barrier barrier(this->nWorkers);

            auto w = [this](int id, Barrier &b)
            {
                this->coordinateWorker(id, b);
            };

            auto t1 = utils::get_timestamp();
            for (int i = 0; i < nWorkers; i++)
            {
                pool.emplace_back(w, i, std::ref(barrier));
            }
            barrier.join();
            barrier.release();

            for (auto &th : pool)
            {
                th.join();
            }
            auto t2 = utils::get_timestamp();
            return (t2 - t1);
        }
    };

}

#endif