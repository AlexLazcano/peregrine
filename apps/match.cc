#include "Peregrine.hh"
#include "mpi.h"
using namespace Peregrine;
void motifs(int size, int world_rank, int world_size, int nthreads, std::vector<SmallGraph> patterns)
{
    DataGraph g("data/citeseer/");

    // std::vector<SmallGraph> patterns = PatternGenerator::all(size, PatternGenerator::VERTEX_BASED, PatternGenerator::INCLUDE_ANTI_EDGES);

    // SmallGraph p1;
    // p1
    //     .add_edge(1, 2)
    //     .add_edge(1, 3)
    //     .add_anti_edge(2, 3)
    //     .set_label(1, 100)
    //     .set_label(2, 101)
    //     .set_label(3, -1);
    //     patterns.emplace_back(p1);

    // SmallGraph p2;
    // p2
    //     .add_edge(1, 2)
    //     .add_anti_edge(2, 3)
    //     .set_label(1, 100);
    //     patterns.emplace_back(p2);

    if (world_rank == 0)
    {
        printf("patterns\n");
        for (const auto &p : patterns)
        {
            std::cout << p << std::endl;
        }
    }

    const auto process = [](auto &&a, auto &&cm)
    { a.map(cm.pattern, 1); };
    auto results = Peregrine::match<Peregrine::Pattern, uint64_t, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(g, patterns, nthreads, process, world_rank, world_size);
    if (world_rank == 0)
    {

        for (const auto &[pattern, count] : results)
        {
            std::cout << "master - " << pattern << ": " << count << std::endl;
        }
    }
}

int main(int argc, char *argv[])
{
    // size_t nthreads = 2;
    int world_rank, world_size;
    int provided;
    int threaded = MPI_THREAD_MULTIPLE;
    MPI_Init_thread(NULL, NULL, threaded, &provided);

    if (provided < threaded)
    {
        std::cerr << "Current Threaded Not supported\n";
        MPI_Finalize();
        return 1;
    }
    const std::string pattern_name(argv[1]);

    size_t nthreads = argc < 3 ? 1 : std::stoi(argv[2]);
    std::vector<Peregrine::SmallGraph> patterns;
    if (auto end = pattern_name.rfind("motifs"); end != std::string::npos)
    {
        auto k = std::stoul(pattern_name.substr(0, end - 1));
        patterns = Peregrine::PatternGenerator::all(k,
                                                    Peregrine::PatternGenerator::VERTEX_BASED,
                                                    Peregrine::PatternGenerator::INCLUDE_ANTI_EDGES);
    }
    else if (auto end = pattern_name.rfind("clique"); end != std::string::npos)
    {
        auto k = std::stoul(pattern_name.substr(0, end - 1));
        patterns.emplace_back(Peregrine::PatternGenerator::clique(k));
    }
    else
    {
        patterns.emplace_back(pattern_name);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    printf("Hello world from process %d, of %d. Thread num: %ld \n", world_rank, world_size, nthreads);

    motifs(4, world_rank, world_size, nthreads, patterns);

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
