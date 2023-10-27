#ifndef VERTEX_COORDINATOR_HH
#define VERTEX_COORDINATOR_HH

#include "mpi.h"
namespace Peregrine
{
    static bool request_range(std::pair<uint64_t, uint64_t> *result)
    {
        MPI_Status status;
        int count;
        uint64_t buffer[2];
        // Tag 0 - Sending empty message to 0
        MPI_Send(&buffer, 0, MPI_UINT64_T, 0, 0, MPI_COMM_WORLD);

        MPI_Probe(0, 1, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_UINT64_T, &count);
        if (count == 0)
        {
            MPI_Recv(&buffer, 0, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            return false;
        }
        else
        {
            MPI_Recv(&buffer, 2, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            *result = std::make_pair(buffer[0], buffer[1]);
            return true;
        }
    }

    class VertexQueue
    {
    private:
        std::atomic<uint64_t> curr = 0;
        uint64_t end;
        int32_t step = 10;
        bool finished = false;
        int number_of_consumers;

    public:
        VertexQueue(uint64_t end, int numConsumers)
        {
            this->number_of_consumers = numConsumers;
            this->end = end;
        }
        bool get_v_range(uint64_t *start_range, uint64_t *end_range)
        {
            while (true)
            {
                if (this->curr >= this->end)
                {
                    *start_range = -1, *end_range = -1;
                    return false;
                }
                uint64_t before = this->curr;
                
                *start_range = before;
                if (before + this->step < this->end)
                {
                    *end_range = before + this->step;
                }
                else
                {
                    *end_range = this->end;
                }

                bool successful = this->curr.compare_exchange_strong(before, *end_range);
                if (successful)
                {
                   break;
                }
                
            }

            return true;
        }
        void coordinate()
        {
            int processesFinished = 0;
            MPI_Status status;
            uint64_t buffer[2];
            while (!this->finished && processesFinished < this->number_of_consumers)
            {
                // Tag 0 - Receive empty message and see who its from
                MPI_Recv(&buffer, 0, MPI_UINT64_T, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
                bool successful = get_v_range(&buffer[0], &buffer[1]);
                if (!successful)
                {
                    // Tag 1 - Ran out of messages to send
                    MPI_Send(&buffer, 0, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);

                    processesFinished++;
                    continue;
                }
                // Tag 1 - Send back range since it was successful
                MPI_Send(&buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            }
        }
    };

}

#endif