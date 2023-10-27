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
        uint64_t curr = 0;
        uint64_t end;
        int32_t step = 11;
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
            if (this->curr >= this->end)
            {
                *start_range = -1, *end_range = -1;
                return false;
            }

            uint64_t start = this->curr;

            *start_range = start;
            if (start + this->step < this->end)
            {
                *end_range = start + this->step;
            }
            else
            {
                *end_range = this->end;
            }

            this->curr = *end_range;
            return true;
        }
        void coordinate()
        {
            int processesFinished = 0;
            MPI_Status status;
            uint64_t buffer[2];
            while (!this->finished && processesFinished < this->number_of_consumers)
            {
                MPI_Recv(&buffer, 0, MPI_UINT64_T, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
                bool successful = get_v_range(&buffer[0], &buffer[1]);
                // printf("start %ld, end %ld \n", buffer[0], buffer[1]);
                if (!successful)
                {
                    MPI_Send(&buffer, 0, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);

                    processesFinished++;
                    continue;
                }
                MPI_Send(&buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
            }
        }
    };

}

#endif