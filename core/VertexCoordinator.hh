#ifndef VERTEX_COORDINATOR_HH
#define VERTEX_COORDINATOR_HH

#include "mpi.h"
namespace Peregrine
{
    static bool request_range(std::pair<uint64_t, uint64_t> result, int world_rank)
    {

        MPI_Status status;
        int count;
        std::vector<uint64_t> buffer(2);
        // Tag 0 - Sending empty message to 0
        MPI_Send(buffer.data(), 1, MPI_UINT64_T, 0, 0, MPI_COMM_WORLD);

        MPI_Probe(0, 1, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_UINT64_T, &count);
        // printf("count %d \n", count );
        if (count == 1)
        {
            printf("count is 1\n");
            // Tag 1 - Returns false since could not get any more ranges
            MPI_Recv(buffer.data(), 1, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            return false;
        }
        else
        {

            // Tag 1 - Got more ranges
            MPI_Recv(buffer.data(), 2, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("Consumer %d:  %ld %ld\n", world_rank, buffer[0], buffer[1]);
            uint64_t first, last;
            first = buffer[0];
            last = buffer[1];
            result = std::make_pair(first, last);
            return true;
        }
    }

    class VertexQueue
    {
    public:
        uint64_t step = 2;
        uint64_t curr = 0;
        uint64_t end;
        bool finished = false;
        int number_of_consumers;
        // std::atomic<uint64_t> curr = 0;
        bool get_v_range(std::pair<uint64_t, uint64_t> &range)
        {
            if (this->curr >= this->end)
            {
                return false;
            }
            uint64_t before = this->curr;
            uint64_t last;

            if (before + this->step < this->end)
            {
                last = before + this->step;
            }
            else
            {
                last = this->end;
            }
            range.first = before;
            range.second = last;
            this->curr += this->step;
            return true;
        }

        VertexQueue(uint64_t last, int numConsumers) : end(last), number_of_consumers(numConsumers)
        {
            printf("ready %ld %d\n", this->end, this->number_of_consumers);
        }

        void coordinate()
        {
            printf("coordinating\n");
            int processesFinished = 0;
            MPI_Status status;
            std::vector<uint64_t> buffer(2);
            std::pair<uint64_t, uint64_t> range;
            bool success;

            while (processesFinished < this->number_of_consumers)
            {
                // Tag 0 - Receive empty message and see who its from
                MPI_Recv(buffer.data(), 1, MPI_UINT64_T, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

                success = this->get_v_range(range);
                printf("range: %ld %ld \n", range.first, range.second);

                if (success)
                {
                   
                    buffer[0] = range.first;
                    buffer[1] = range.second;
                    MPI_Send(buffer.data(), 2, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);

                }
                else
                {
                    
                    MPI_Send(buffer.data(), 1, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                    processesFinished++;
                }
            }
        }
    };

}

#endif