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
    public:
        RangeQueue(/* args */);
        ~RangeQueue();
        void addRange(Range r);
        std::optional<Range> popLastRange();
        std::optional<Range> popFirstRange();
        void resetVector();
        void printRanges(int world_rank);
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

    inline void RangeQueue::printRanges(int world_rank)
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        for (const auto &r : this->range_queue) {
        printf("Range Queue: %d: %ld %ld \n", world_rank, r.first, r.second);
      }
    }

    RangeQueue::RangeQueue(/* args */)
    {
    }
    
    RangeQueue::~RangeQueue()
    {
    }
    
} // namespace Peregrine

#endif