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
        uint64_t step = 100;
        uint64_t curr = 0;
        uint64_t number_tasks = 0;
        int number_of_consumers;

    public:
        void reset_curr()
        {
            this->curr = 0;
        }
        void update_number_tasks(uint64_t numberTasks)
        {
            this->number_tasks = numberTasks;
        }
        void update_step(u_int64_t new_step) {
            this->step = new_step;
        }
        bool get_v_range(std::pair<uint64_t, uint64_t> &range)
        {
            if (this->curr >= this->number_tasks)
            {
                return false;
            }
            uint64_t before = this->curr;
            uint64_t last;

            if (before + this->step < this->number_tasks)
            {
                last = before + this->step;
            }
            else
            {
                last = this->number_tasks;
            }
            range.first = before;
            range.second = last;
            this->curr += this->step;
            return true;
        }

        VertexCoordinator(int numConsumers) : number_of_consumers(numConsumers)
        {
        }
        VertexCoordinator(int numConsumers, int64_t steps_init) : number_of_consumers(numConsumers), step(steps_init)
        {
        }

        void coordinate()
        {
            // printf("coordinating\n");
            int processesFinished = 0;
            MPI_Status status;
            // std::vector<uint64_t> buffer(2);
            uint64_t buffer[2];
            std::pair<uint64_t, uint64_t> range;
            bool success;
            // printf("number of conusumer: %d tasks %ld\n", this->number_of_consumers, this->number_tasks);
            while (processesFinished < this->number_of_consumers)
            {
                // Tag 0 - Receive empty message and see who its from
                MPI_Recv(buffer, 1, MPI_UINT64_T, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

                success = this->get_v_range(range);

                if (success)
                {

                    buffer[0] = range.first;
                    buffer[1] = range.second;
                    // Tag 1 - size 2
                    MPI_Send(buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                }
                else
                {
                    // Tag 1
                    MPI_Send(buffer, 1, MPI_UINT64_T, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
                    processesFinished++;
                    // printf("p finished %d / %d\n", processesFinished, this->number_of_consumers);
                }
            }
        }
    };

}

#endif