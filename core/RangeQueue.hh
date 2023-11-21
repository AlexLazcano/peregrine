#ifndef RANGES_QUEUE_HH
#define RANGES_QUEUE_HH

#include <mutex>
#include <tbb/concurrent_queue.h>

const int MPI_STEAL_CHANNEL = 5;
const int MPI_STOLEN_CHANNEL = 6;
const int MPI_DONE_CHANNEL = 10;

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

bool elementExists(std::vector<int> &processes, int rankToFind)
{
    auto it = std::find_if(processes.begin(), processes.end(), [rankToFind](const auto &rank)
                           { return rank == rankToFind; });

    if (it != processes.end())
    {
        return true;
    }
    return false;
}
int findLowest(std::vector<int> &processes, int notThisOne)
{
    if (processes.empty())
    {
        return -1; // Indicate that the vector is empty
    }

    auto it = std::find_if(processes.begin(), processes.end(), [notThisOne](const auto &rank)
                           { return rank != notThisOne; });

    if (it != processes.end())
    {
        return *it; // Return the found value
    }

    return -1; // Indicate that the value was not found
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
        tbb::concurrent_queue<Range> concurrent_range_queue;
        int world_rank;
        int world_size;
        uint32_t nWorkers;
        std::vector<int> activeProcesses;
        std::vector<int> signals;
        // std::vector<Range> work_range;
        std::vector<MPI_Request> send_reqs;
        std::vector<MPI_Request> recv_reqs;
        MPI_Request robber_req;
        MPI_Request stolen_req;
        int robber_buffer;
        MPI_Status robber_status;
        bool done_requesting = false;

    public:
        bool done_ranges_given = false;
        bool done_stealing = false;
        int get_rank();
        RangeQueue(int world_rank, int world_size, uint32_t nworkers);
        ~RangeQueue();
        void addRange(Range r);
        bool fetchWorker();
        std::optional<Range> popRange();
        void resetVector();
        void printRanges();
        bool stealRange();
        bool stealRangeAsync();
        bool recvStolen(uint64_t *buffer, const int activeRank, MPI_Status *status);
        bool recvStolenAsync(uint64_t *buffer, MPI_Status *status);
        void initRobbers();
        void finishRobbers();
        int checkRobbers();
        bool handleRobbers();
        bool isQueueEmpty();
        std::optional<Range> request_range();
        void split_addRange(Range range, uint split);
        void openSignal();
        void signalDone();
        bool handleSignal();
        void showActive();
        // void showWork();
        void printRecv();
        bool waitAllSends();
        bool getDoneRequesting();
        bool noMoreActive();
        void setDoneRequesting(bool b);
        void coordinateScatter(Range full_range);
        std::vector<uint64_t> splitRangeForScatter(Range range);
    };

    void RangeQueue::coordinateScatter(Range range)
    {
        uint split = world_size;
        uint64_t recv[2] = {0, 0};

        if (world_rank == 0)
        {
            std::vector<Range> sections;

            printf("Full Range: %ld %ld\n", range.first, range.second);

            if (split == 0)
            {
                throw std::invalid_argument("ERROR: split arg cannot be 0\n");
            }

            // printf("Split: %d\n", split);
            // printf("Range: %ld %ld\n", range.first, range.second);

            uint64_t span = range.second - range.first;

            uint64_t elementsPerSplit = span / split;

            if (span < split)
            {
                // printf("Range too small\n");
                sections.emplace_back(range);
                return;
            }
            // Initialize variables for tracking the current sub-range
            uint64_t currentStart = range.first;
            uint64_t currentEnd = currentStart + elementsPerSplit;

            // Loop to create and process each sub-range
            for (uint i = 0; i < split; ++i)
            {
                // Ensure the last sub-range covers any remaining elements
                if (i == split - 1)
                {
                    currentEnd = range.second;
                }

                // Process the current sub-range (you can replace this with your specific logic)
                // printf("RANK %d Sub-Range %d: %ld %ld\n", i + 1, world_rank, currentStart, currentEnd);

                sections.emplace_back(Range(currentStart, currentEnd));
                // Update for the next sub-range
                currentStart = currentEnd;
                currentEnd = currentStart + elementsPerSplit;
            }
            int i = 0;
            for (const auto &s : sections)
            {

                // printf("i: %d - (%ld %ld)\n", i, s.first, s.second);
                auto buffer = splitRangeForScatter(s);

                auto array = buffer.data();

                MPI_Scatter(array, 2, MPI_UINT64_T, &recv, 2, MPI_UINT64_T, 0, MPI_COMM_WORLD);
                // printf("Rank %d recv %ld %ld\n", world_rank, recv[0], recv[1]);
                split_addRange(Range(recv[0], recv[1]), nWorkers);
                i++;
                // MPI_Barrier(MPI_COMM_WORLD);
            }
        }
        else
        {
            uint64_t dummy;

            for (int i = 0; i < world_size; i++)
            {

                MPI_Scatter(&dummy, 2, MPI_UINT64_T, &recv, 2, MPI_UINT64_T, 0, MPI_COMM_WORLD);
                // printf("Rank %d recv %ld %ld\n", world_rank, recv[0], recv[1]);
                split_addRange(Range(recv[0], recv[1]), nWorkers);
                // MPI_Barrier(MPI_COMM_WORLD);
            }
        }
    }
    std::vector<uint64_t> RangeQueue::splitRangeForScatter(Range range)
    {
        std::vector<uint64_t> ranges;
        uint split = world_size;
        // printf("Range: %ld %ld\n", range.first, range.second);

        if (split == 0)
        {
            throw std::invalid_argument("ERROR: split arg cannot be 0\n");
        }

        // printf("Split: %d\n", split);
        // printf("Range: %ld %ld\n", range.first, range.second);

        uint64_t span = range.second - range.first;

        uint64_t elementsPerSplit = span / split;

        if (span < split)
        {
            // printf("Range too small\n");
            ranges.emplace_back(range.first);
            ranges.emplace_back(range.second);
            return ranges;
        }
        // Initialize variables for tracking the current sub-range
        uint64_t currentStart = range.first;
        uint64_t currentEnd = currentStart + elementsPerSplit;

        // Loop to create and process each sub-range
        for (uint i = 0; i < split; ++i)
        {
            // Ensure the last sub-range covers any remaining elements
            if (i == split - 1)
            {
                currentEnd = range.second;
            }

            // Process the current sub-range (you can replace this with your specific logic)
            // printf("RANK %d Sub-Range %d: %ld %ld\n", i + 1, world_rank, currentStart, currentEnd);

            ranges.emplace_back(currentStart);
            ranges.emplace_back(currentEnd);
            // Update for the next sub-range
            currentStart = currentEnd;
            currentEnd = currentStart + elementsPerSplit;
        }
        return ranges;
    }

    int RangeQueue::get_rank()
    {
        return world_rank;
    }
    bool RangeQueue::noMoreActive()
    {
        return activeProcesses.size() == 0;
    }
    bool RangeQueue::getDoneRequesting()
    {
        return done_requesting;
    }
    void RangeQueue::setDoneRequesting(bool b)
    {
        done_requesting = b;
    }
    bool RangeQueue::fetchWorker()
    {

        while (true)
        {
            auto maybeRange = this->request_range();

            if (!maybeRange.has_value())
            {
                this->done_requesting = true;
                break;
            }
            auto range = maybeRange.value();

            // printf("Rank %d recv %ld %ld %d\n", world_rank, range.first, range.second, nWorkers);

            this->split_addRange(range, this->nWorkers * (this->world_size - 1) * 10);
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
        return true;
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
    void Peregrine::RangeQueue::split_addRange(Range range, uint split)
    {
        if (split == 0)
        {
            throw std::invalid_argument("ERROR: split arg cannot be 0\n");
        }

        // printf("Split: %d\n", split);
        // printf("Range: %ld %ld\n", range.first, range.second);

        uint64_t span = range.second - range.first;

        uint64_t elementsPerSplit = span / split;

        if (span < split)
        {
            // printf("Range too small\n");
            this->addRange(range);
            return;
        }
        // Initialize variables for tracking the current sub-range
        uint64_t currentStart = range.first;
        uint64_t currentEnd = currentStart + elementsPerSplit;

        // Loop to create and process each sub-range
        for (uint i = 0; i < split; ++i)
        {
            // Ensure the last sub-range covers any remaining elements
            if (i == split - 1)
            {
                currentEnd = range.second;
            }

            // Process the current sub-range (you can replace this with your specific logic)
            // printf("RANK %d Sub-Range %d: %ld %ld\n", i + 1, world_rank, currentStart, currentEnd);
            this->addRange(Range(currentStart, currentEnd));

            // Update for the next sub-range
            currentStart = currentEnd;
            currentEnd = currentStart + elementsPerSplit;
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
                MPI_Irecv(&signals[i], 1, MPI_INT, src, MPI_DONE_CHANNEL, MPI_COMM_WORLD, &recv_reqs[i]);
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
                MPI_Isend(&signal, 1, MPI_INT, dest, MPI_DONE_CHANNEL, MPI_COMM_WORLD, &send_reqs[i]);
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
        if (flag && activeProcesses.size() == 0)
        {
            // printf("RANK %d ALL DONE\n", world_rank);
            return true;
        }

        return false;
    }

    // void RangeQueue::showWork()
    // {
    //     double count = 0.0;

    //     // for (auto const &[first, second] : work_range)
    //     // {
    //     //     count += 1.0;
    //     //     printf("Rank %d: %ld %ld \n", world_rank, first, second);
    //     // }
    //     double sum;

    //     // MPI_Allreduce(&count, &sum, 1, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);
    //     // if (world_rank == 0)
    //     // {
    //     //     return;
    //     // }

    //     // double percent = count / (sum * world_size);

    //     // MPI_Barrier(MPI_COMM_WORLD);
    //     // printf("Work done by each process: %.1f / %.0f = %.1f  \n", count, sum * world_size, percent * 100);
    // }
    inline void RangeQueue::showActive()
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
        concurrent_range_queue.push(r);
    }

    std::optional<Range> RangeQueue::popRange()
    {
        Range value;
        bool success = concurrent_range_queue.try_pop(value);
        if (success)
        {
            return value;
        }
        if (!done_ranges_given)
        {
            // printf("Rank %d done ranges\n", world_rank);
            signalDone();
            done_ranges_given = true;
        }

        return std::nullopt;
    }

    inline void RangeQueue::resetVector()
    {
        this->done_requesting = false;
        this->done_ranges_given = false;
        this->done_stealing = false;
        concurrent_range_queue.clear();
    }

    inline void RangeQueue::printRanges()
    {
        // std::lock_guard<std::mutex> lock(this->mtx);
        typedef tbb::concurrent_queue<Range>::const_iterator iter;
        for (iter i(concurrent_range_queue.unsafe_begin()); i != concurrent_range_queue.unsafe_end(); ++i)
        {
            auto r = *i;
            printf("Range Queue: %d: %ld %ld \n", world_rank, r.first, r.second);
        }
    }

    bool Peregrine::RangeQueue::stealRange()
    {
        uint64_t buffer[2];
        MPI_Status status;

        if (activeProcesses.size() == 0)
        {
            return true;
        }

        for (const auto &activeRank : activeProcesses)
        {
            if (activeRank == world_rank) // || activeRank == 0)
            {
                continue;
            }

            printf("RANK %d stealing from %d\n", world_rank, activeRank);
            MPI_Send(&buffer, 1, MPI_UINT64_T, activeRank, MPI_STEAL_CHANNEL, MPI_COMM_WORLD);
            recvStolen(buffer, activeRank, &status);
        }
        return false;
    }

    bool RangeQueue::stealRangeAsync()
    {
        uint64_t buffer[2];
        MPI_Request request;
        MPI_Status status;
        auto lowestRank = findLowest(activeProcesses, world_rank);

        if (lowestRank == -1)
        {
            return false;
        }

        // printf("Rank %d attemting stealing from %d\n", world_rank, lowestRank);
        MPI_Isend(&buffer, 1, MPI_UINT64_T, lowestRank, MPI_STEAL_CHANNEL, MPI_COMM_WORLD, &request);

        bool didReceive = recvStolenAsync(buffer, &status);
        return didReceive;
    }

    bool RangeQueue::recvStolenAsync(uint64_t *buffer, MPI_Status *status)
    {
        int count;
        int flag = 0;
        // printf("Rank %d probing\n", world_rank);
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, &flag, status);

        if (flag)
        {

            MPI_Get_count(status, MPI_UINT64_T, &count);

            if (count == 1)
            {
                // Tag 6 - Returns false since could not get any more ranges
                MPI_Recv(buffer, 1, MPI_UINT64_T, status->MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // printf("Rank %d couldnt steal\n", world_rank);
            }
            else
            {
                // Tag 6 - Got more ranges
                MPI_Recv(buffer, 2, MPI_UINT64_T, status->MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                printf("RANK %d stole (%ld %ld) from: %d \n", world_rank, buffer[0], buffer[1], status->MPI_SOURCE);

                this->addRange(Range(buffer[0], buffer[1]));
            }
            return true;
        }
        return false;
    }

    bool RangeQueue::recvStolen(uint64_t *buffer, const int activeRank, MPI_Status *status)
    {
        int count;
        int success = MPI_Probe(activeRank, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, status);
        // printf("Probing %d - %d\n", success, status->MPI_ERROR);
        MPI_Get_count(status, MPI_UINT64_T, &count);
        // printf("count stuck\n");

        if (count == 1)
        {
            // Tag 6 - Returns false since could not get any more ranges
            MPI_Recv(buffer, 1, MPI_UINT64_T, status->MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("Rank %d coudnt steal\n", world_rank);
        }
        else
        {
            // Tag 6 - Got more ranges
            MPI_Recv(buffer, 2, MPI_UINT64_T, status->MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // printf("RANK %d recv %ld %ld\n", world_rank, buffer[0], buffer[1]);

            this->addRange(Range(buffer[0], buffer[1]));
        }
        return true;
    }

    void RangeQueue::initRobbers()
    {
        int count = 1;
        MPI_Irecv(&robber_buffer, count, MPI_UINT64_T, MPI_ANY_SOURCE, MPI_STEAL_CHANNEL, MPI_COMM_WORLD, &robber_req);
    }
    void RangeQueue::finishRobbers()
    {
        if (!checkRobbers())
        {

            MPI_Cancel(&robber_req);
            MPI_Wait(&robber_req, MPI_STATUS_IGNORE);
        }
    }

    int RangeQueue::checkRobbers()
    {
        // printf("Rank %d checking for robbers\n", world_rank);
        int flag = 0;
        MPI_Test(&robber_req, &flag, &robber_status);

        return flag;
    }

    bool RangeQueue::handleRobbers()
    {

        auto maybeRange = this->popRange();
        uint64_t buffer[2] = {0, 0};
        if (!maybeRange.has_value())
        {
            // printf("Rank %d sending 1\n", world_rank);
            MPI_Send(buffer, 1, MPI_UINT64_T, robber_status.MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD);
            initRobbers();
            return false;
        }

        auto range = maybeRange.value();
        buffer[0] = range.first;
        buffer[1] = range.second;

        // printf("RANK %d sending to %d : (%ld %ld) \n", world_rank, robber_status.MPI_SOURCE, buffer[0], buffer[1]);
        MPI_Send(buffer, 2, MPI_UINT64_T, robber_status.MPI_SOURCE, MPI_STOLEN_CHANNEL, MPI_COMM_WORLD);
        initRobbers();

        return true;
    }
    inline bool RangeQueue::isQueueEmpty()
    {
        return this->concurrent_range_queue.empty();
    }
    RangeQueue::RangeQueue(int world_rank, int world_size, uint32_t nworkers = 1)
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