#ifndef RANGES_QUEUE_HH
#define RANGES_QUEUE_HH

#include <mutex>

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
        // TODO: Can make it better by stealing from back and having separate mtx?
        // std::vector<std::pair<uint64_t, uint64_t>> range_vector;
        std::deque<Range> range_queue;
        // bool *finishedProcesses;
        int world_rank;
        int world_size;
        std::vector<int> activeProcesses;
        std::vector<int> buffers;
        std::vector<MPI_Request> bcast_requests;

    public:
        RangeQueue(int world_rank, int world_size);
        ~RangeQueue();
        void addRange(Range r);
        std::optional<Range> popLastRange();
        std::optional<Range> popFirstRange();
        void resetVector();
        void printRanges();
        std::optional<Range> stealRange();
        void initRobbers(MPI_Request &req, uint64_t *buffer);
        int checkRobbers(MPI_Status &status, MPI_Request &req);
        bool handleRobbers(MPI_Status &status, MPI_Request &req, uint64_t *buffer);
        bool isQueueEmpty();
        std::optional<Range> request_range();
        void broadcastFinished();
        bool handleBcasts();
        void printActive();
    };

    std::optional<Range> RangeQueue::request_range()
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

            return std::nullopt;
        }
        else
        {
            // Tag 1 - Got more ranges
            MPI_Recv(buffer, 2, MPI_UINT64_T, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            return Range(buffer[0], buffer[1]);
        }
    }

    void RangeQueue::broadcastFinished()
    {
        std::vector<int> buffers_local(activeProcesses.size());

        for (const auto &rank : activeProcesses)
        {
            buffers_local[rank] = rank;
            // printf("%d rank: %d\n", world_rank, rank);
            MPI_Ibcast(&buffers_local[rank], 1, MPI_INT, rank, MPI_COMM_WORLD, &bcast_requests[rank]);
            // printf("RANK %d: waiting for %d - req %d\n", world_rank, rank, req);
        }

        buffers = std::move(buffers_local);
    }


    bool RangeQueue::handleBcasts()
    {

        int count = bcast_requests.size();

        std::vector<MPI_Status> statuses(count);

        std::vector<int> indices(count);
        int successCount = 0;

        MPI_Request *array = bcast_requests.data();

        MPI_Testsome(count, array, &successCount, indices.data(), statuses.data());

        if (successCount > 0)
        {

            for (int i = 0; i < successCount; i++)
            {
                int completed = indices[i];
                int rank_done = buffers[completed];

                printf("RANK %d completed: %d\n", world_rank, rank_done);
                findAndRemoveElement(activeProcesses, rank_done);

            }
            int allFinished = 0;
            MPI_Testall(count, array, &allFinished, statuses.data());
            if (allFinished)
            {

                return true;
            }
            return false;
        }

        return false;
    }

    inline void RangeQueue::printActive()
    {
        for (const auto& a : activeProcesses)
        {
            printf("Rank %d: Active %d\n",world_rank, a);
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
        std::lock_guard<std::mutex> lock(this->mtx);
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

    std::optional<Range> Peregrine::RangeQueue::stealRange()
    {
        uint64_t buffer[2];
        MPI_Status status;
        int count;
        for (int i = 0; i < this->world_size; i++)
        {
            if (i == world_rank)
            {
                continue;
            }
            printf("sent message %d / %d\n", i, this->world_size);
            MPI_Send(&buffer, 1, MPI_UINT64_T, i, 5, MPI_COMM_WORLD);
            MPI_Probe(i, 6, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_UINT64_T, &count);
            if (count == 1)
            {
                // Tag 6 - Returns false since could not get any more ranges
                MPI_Recv(buffer, 1, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                continue;
            }
            else
            {
                // Tag 6 - Got more ranges
                MPI_Recv(buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                return Range(buffer[0], buffer[1]);
            }
        }
        return std::nullopt;
    }

    void RangeQueue::initRobbers(MPI_Request &req, uint64_t *buffer)
    {
        int count = 1;
        MPI_Irecv(buffer, count, MPI_UINT64_T, MPI_ANY_SOURCE, 5, MPI_COMM_WORLD, &req);
    }

    int RangeQueue::checkRobbers(MPI_Status &status, MPI_Request &req)
    {
        int flag = 0;
        MPI_Test(&req, &flag, &status);

        return flag;
    }

    bool RangeQueue::handleRobbers(MPI_Status &status, MPI_Request &req, uint64_t *buffer)
    {

        auto maybeRange = this->popFirstRange();

        if (!maybeRange.has_value())
        {
            printf("sending 1\n");
            MPI_Send(buffer, 1, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD);
            return false;
        }

        auto range = maybeRange.value();
        printf("%ld %ld\n", range.first, range.second);
        buffer[0] = range.first;
        buffer[1] = range.second;

        MPI_Send(buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD);

        return true;
    }
    inline bool RangeQueue::isQueueEmpty()
    {
        return this->range_queue.empty();
    }

    RangeQueue::RangeQueue(int world_rank, int world_size)
    {
        this->world_rank = world_rank;
        this->world_size = world_size;
        for (int i = 0; i < this->world_size; i++)
        {
            // if (i == world_rank)
            // {
            //     continue;
            // }

            this->activeProcesses.emplace_back(i);
        }
        this->bcast_requests.resize(world_size);
    }

    RangeQueue::~RangeQueue()
    {
        // delete[] this->finishedProcesses;
    }

} // namespace Peregrine

#endif