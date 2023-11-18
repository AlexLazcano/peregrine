#ifndef RANGES_QUEUE_HH
#define RANGES_QUEUE_HH

#include <mutex>
#include <tbb/concurrent_queue.h>

const int MPI_STEAL_CHANNEL = 5;
const int MPI_STOLEN_CHANNEL = 6;

// Function to find and remove an element from the vector
void findAndRemoveElement(std::vector<int> &processes, int rankToRemove)
{
    auto it = std::find_if(processes.begin(), processes.end(), [rankToRemove](const auto &rank)
                           { return rank == rankToRemove; });

    if (it != processes.end())
    {
        processes.erase(it);
    }
}
namespace Peregrine
{
    using Range = std::pair<uint64_t, uint64_t>;
    using Request_Vector = std::vector<MPI_Request>;

    class RangeQueue
    {
    private:
        std::mutex mtx;
        std::mutex rangeMtx;
        // TODO: Can make it better by stealing from back and having separate mtx?
        // std::vector<std::pair<uint64_t, uint64_t>> range_vector;
        std::deque<Range> range_queue;
        // bool *finishedProcesses;
        int world_rank;
        int world_size;
        uint32_t nWorkers;
        std::vector<int> activeProcesses;
        std::vector<int> signals;
        std::vector<MPI_Request> send_reqs;
        std::vector<MPI_Request> recv_reqs;
        MPI_Request robber_req;
        MPI_Request stolen_req;
        int robber_buffer;
        MPI_Status robber_status;

    public:
        RangeQueue(int world_rank, int world_size, uint32_t nworkers);
        ~RangeQueue();
        void addRange(Range r);
        void fetchWorker(uint64_t num_tasks);
        std::optional<Range> popLastRange();
        std::optional<Range> popFirstRange();
        void resetVector();
        void printRanges();
        bool stealRange();
        void initRobbers();
        int checkRobbers();
        bool handleRobbers();
        bool isQueueEmpty();
        std::optional<Range> request_range();
        void openSignal();
        void signalDone();
        bool handleSignal();
        void printActive();
        void printRecv();
        bool waitAllSends();
    };

    void RangeQueue::fetchWorker(uint64_t num_tasks)
    {
        (void) num_tasks;

        while (true)
        {
            auto maybeRange = this->request_range();

            if (!maybeRange.has_value())
            {
                break;
            }
            auto range = maybeRange.value();

            // printf("Rank %d recv %ld %ld %ld\n", world_rank, range.first, range.second, num_tasks);

            this->addRange(range);
        }
    }

    std::optional<Range> RangeQueue::request_range()
    {
        std::lock_guard<std::mutex> lock(this->rangeMtx);
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

            return std::nullopt;
        }
        else
        {
            // Tag 1 - Got more ranges
            MPI_Recv(buffer, 2, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            return Range(buffer[0], buffer[1]);
        }
    }

    void RangeQueue::openSignal()
    {
        // Start non-blocking receives from all other processes
        int i = 0;
        for (int src = 0; src < world_size; src++)
        {
            if (src != world_rank)
            {
                // printf("RANK %d recv %d in i: %i\n", world_rank, src, i);
                MPI_Irecv(&signals[i], 1, MPI_INT, src, 10, MPI_COMM_WORLD, &recv_reqs[i]);
                i++;
            }
        }
    }

    void RangeQueue::signalDone()
    {
        // Start non-blocking sends to all other processes
        int i = 0;
        for (int dest = 0; dest < world_size; dest++)
        {
            if (dest != world_rank)
            {
                int signal = this->world_rank;
                MPI_Isend(&signal, 1, MPI_INT, dest, 10, MPI_COMM_WORLD, &send_reqs[i]);
                // printf("RANK %d send %d in i: %d - %d \n", world_rank, dest, i, send_reqs[i]);
                i++;
            }
        }
        findAndRemoveElement(this->activeProcesses, this->world_rank);
    }

    bool RangeQueue::waitAllSends()
    {
        int count = send_reqs.size();

        MPI_Waitall(count, send_reqs.data(), MPI_STATUS_IGNORE);
        return true;
    }

    bool RangeQueue::handleSignal()
    {
        int count = recv_reqs.size();

        std::vector<MPI_Status> statuses(count);

        std::vector<int> indices(count);

        MPI_Request *array = recv_reqs.data();

        int i = 0;
        for (int src = 0; src < world_size; src++)
        {
            if (src != world_rank)
            {
                int flag = 0;
                // printf("RANK %d recv %d in i: %i\n", world_rank, src, i);
                MPI_Test(&recv_reqs[i], &flag, MPI_STATUS_IGNORE);

                if (flag)
                {
                    // printf("RANK %d completed: %d\n", world_rank, signals[i]);
                    findAndRemoveElement(activeProcesses, signals[i]);
                }
                i++;
            }
        }

        int flag = 0;
        MPI_Testall(count, array, &flag, MPI_STATUS_IGNORE);
        if (flag)
        {
            // printf("RANK %d ALL DONE\n", world_rank);
            return true;
        }

        return false;
    }

    inline void RangeQueue::printActive()
    {
        printf("RANK: %d Active size: %ld\n", world_rank, activeProcesses.size());
        for (const auto &a : activeProcesses)
        {
            printf("Rank %d: Active process %d \n", world_rank, a);
        }
    }
    inline void RangeQueue::printRecv()
    {
        printf("Active size: %ld\n", recv_reqs.size());
        for (const auto &r : recv_reqs)
        {
            printf("Rank %d: Active process %d \n", world_rank, r);
        }
    }

    void RangeQueue::addRange(Range r)
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        range_queue.emplace_back(r);
    }

    std::optional<Range> RangeQueue::popLastRange()
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        if (range_queue.empty())
        {
            return std::nullopt;
        }

        Range last = range_queue.back();
        range_queue.pop_back();
        return last;
    }

    inline std::optional<Range> RangeQueue::popFirstRange()
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        if (this->range_queue.empty())
        {
            return std::nullopt;
        }
        Range first = range_queue.front();

        range_queue.pop_front();
        return first;
    }

    inline void RangeQueue::resetVector()
    {
        range_queue.clear();
    }

    inline void RangeQueue::printRanges()
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        for (const auto &r : this->range_queue)
        {
            printf("Range Queue: %d: %ld %ld \n", world_rank, r.first, r.second);
        }
    }

    bool Peregrine::RangeQueue::stealRange()
    {
        uint64_t buffer[2];
        MPI_Status status;
        int count;
        if (activeProcesses.size() == 0)
        {
            return true;
        }
        

        for (const auto &activeRank : activeProcesses)
        {
            if (activeRank == world_rank)
            {
               continue;
            }
            
            printf("RANK %d stealing from %d\n", world_rank, activeRank);
            MPI_Send(&buffer, 1, MPI_UINT64_T, activeRank, MPI_STEAL_CHANNEL, MPI_COMM_WORLD);
            MPI_Probe(activeRank, 6, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_UINT64_T, &count);
            if (count == 1)
            {
                // Tag 6 - Returns false since could not get any more ranges
                MPI_Recv(buffer, 1, MPI_UINT64_T, status.MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                continue;
            }
            else
            {
                // Tag 6 - Got more ranges
                MPI_Recv(buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                printf("RANK %d recv %ld %ld\n", world_rank, buffer[0], buffer[1]);

                this->addRange(Range(buffer[0], buffer[1]));
            }
        }
        return false;
    }

    void RangeQueue::initRobbers()
    {
        int count = 1;
        MPI_Irecv(&robber_buffer, count, MPI_UINT64_T, MPI_ANY_SOURCE, MPI_STEAL_CHANNEL, MPI_COMM_WORLD, &robber_req);
    }

    int RangeQueue::checkRobbers()
    {
        int flag = 0;
        MPI_Test(&robber_req, &flag, &robber_status);

        return flag;
    }

    bool RangeQueue::handleRobbers()
    {

        auto maybeRange = this->popFirstRange();
        uint64_t buffer[2] = {0, 0};
        if (!maybeRange.has_value())
        {
            printf("sending 1\n");
            MPI_Send(buffer, 1, MPI_UINT64_T, robber_status.MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD);
            return false;
        }

        auto range = maybeRange.value();
        buffer[0] = range.first;
        buffer[1] = range.second;

        printf("RANK %d sending to %d : (%ld %ld) \n", world_rank, robber_status.MPI_SOURCE, buffer[0], buffer[1]);
        MPI_Send(buffer, 2, MPI_UINT64_T, robber_status.MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD);

        return true;
    }
    inline bool RangeQueue::isQueueEmpty()
    {
        return this->range_queue.empty();
    }
    RangeQueue::RangeQueue(int world_rank, int world_size, uint32_t nworkers = 1 )
    {
        this->world_rank = world_rank;
        this->world_size = world_size;
        this->nWorkers = nworkers;
        for (int i = 0; i < this->world_size; i++)
        {
            // if (i == world_rank)
            // {
            //     continue;
            // }

            this->activeProcesses.emplace_back(i);
        }
        this->send_reqs.resize(world_size - 1, MPI_REQUEST_NULL);
        this->recv_reqs.resize(world_size - 1, MPI_REQUEST_NULL);
        this->signals.resize(world_size);
    }

    RangeQueue::~RangeQueue()
    {
        // delete[] this->finishedProcesses;
    }

} // namespace Peregrine

#endif