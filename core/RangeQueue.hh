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
    std::vector<std::pair<uint64_t, uint64_t>> range_vector;
    public:
        RangeQueue(/* args */);
        ~RangeQueue();
        void addRange(Range r);
        Range popLastRange();
        void resetVector();
        void printRanges(int world_rank);
    };

    void RangeQueue::addRange(Range r) {
        std::lock_guard<std::mutex> lock(this->mtx);
        range_vector.emplace_back(r);
    }

    Range RangeQueue::popLastRange() {
        std::lock_guard<std::mutex> lock(this->mtx);
        if (range_vector.empty())
        {
            throw "Range Vector is empty";
        }

        Range last = range_vector.back();
        range_vector.pop_back();
        return last;
        
    }

    inline void RangeQueue::resetVector()
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        range_vector.clear();
    }

    inline void RangeQueue::printRanges(int world_rank)
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        for (const auto &r : this->range_vector) {
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