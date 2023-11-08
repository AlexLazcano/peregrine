#ifndef RANGES_QUEUE_HH
#define RANGES_QUEUE_HH

#include <mutex>

namespace Peregrine
{
    using Range = std::pair<uint64_t, uint64_t>;

    class RangeQueue
    {
    private:
    std::mutex mtx;
    // TODO: Can make it better by stealing from back and having separate mtx? 
    // std::vector<std::pair<uint64_t, uint64_t>> range_vector;
    std::deque<Range> range_queue;
    bool *finishedProcesses;
    int world_rank;
    int world_size;

    public:
        RangeQueue(int world_rank, int world_size);
        ~RangeQueue();
        void addRange(Range r);
        std::optional<Range> popLastRange();
        std::optional<Range> popFirstRange();
        void resetVector();
        void printRanges();
        std::optional<Range> stealRange();
        void checkRobbers();
        bool isQueueEmpty();
    };

    void RangeQueue::addRange(Range r) {
        std::lock_guard<std::mutex> lock(this->mtx);
        range_queue.emplace_back(r);
    }

    std::optional<Range> RangeQueue::popLastRange() {
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
        for (const auto &r : this->range_queue) {
        printf("Range Queue: %d: %ld %ld \n", world_rank, r.first, r.second);
      }
    }

    std::optional<Range> Peregrine::RangeQueue::stealRange()
    {
    
        uint64_t buffer[2];
        MPI_Status status;
        int count;
        for (int i = 0; i < world_size; i++)
        {
            if (i == world_rank)
            {
                continue;
            }

            MPI_Send(&buffer, 1, MPI_UINT64_T, i, 5, MPI_COMM_WORLD);
            MPI_Probe(i, 6, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_UINT64_T, &count);
            if (count == 1)
            {
                // Tag 6 - Returns false since could not get any more ranges
                MPI_Recv(buffer, 1, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                return std::nullopt;
            }
            else
            {
                // Tag 6 - Got more ranges
                MPI_Recv(buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                auto result = std::make_pair(buffer[0], buffer[1]);
                return result;
            }

            printf("trying to steal from process %d\n", i);
        }

        return std::optional<Range>();
    }

    void RangeQueue::checkRobbers()
    {
        MPI_Status status;
        int64_t buffer[2] = {0};

        MPI_Recv(buffer, 1, MPI_UINT64_T, MPI_ANY_SOURCE, 5, MPI_COMM_WORLD, &status);

        auto maybeRange = popLastRange();

        if (maybeRange.has_value())
        {

            Range range = maybeRange.value();
            buffer[0] = range.first;
            buffer[1] = range.second;

            MPI_Send(buffer, 2, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD);
        }
        else
        { 
            MPI_Send(buffer, 1, MPI_UINT64_T, status.MPI_SOURCE, 6, MPI_COMM_WORLD);
        }
    }

    inline bool RangeQueue::isQueueEmpty()
    {
        return this->range_queue.empty();
    }

    RangeQueue::RangeQueue(int world_rank, int world_size)
    {
        this->world_rank = world_rank;
        this->world_size = world_rank;
        this->finishedProcesses = new bool[world_size];
    }
    
    RangeQueue::~RangeQueue()
    {
        delete[] this->finishedProcesses;
    }
    
} // namespace Peregrine

#endif