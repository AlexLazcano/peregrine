#include "Peregrine.hh"
#include "mpio.h"
using namespace Peregrine;
void motifs(int size, int world_rank, int world_size, int nthreads)
{
    DataGraph g("data/citeseer/");

    std::vector<SmallGraph> patterns = PatternGenerator::all(size, PatternGenerator::VERTEX_BASED, PatternGenerator::INCLUDE_ANTI_EDGES);

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
            std::cout << pattern << ": " << count << std::endl;
        }
    }
}

int main()
{
    size_t nthreads = 1;
    int world_rank, world_size;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    printf("Hello world from process %d, of %d. Thread num: %ld \n", world_rank, world_size, nthreads);

    motifs(4, world_rank, world_size, nthreads);

    printf("DONE Process %d\n", world_rank);

    MPI_Finalize();
    return 0;
}
