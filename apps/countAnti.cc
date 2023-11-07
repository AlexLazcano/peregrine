#include "Peregrine.hh"
#include "mpio.h"
using namespace Peregrine;
void motifs(int size, int world_rank, int world_size, int nthreads)
{
  DataGraph g("data/citeseer/");

  std::vector<SmallGraph> graphs = PatternGenerator::all(size, PatternGenerator::EDGE_BASED, PatternGenerator::INCLUDE_ANTI_EDGES);

  auto results = count(g, graphs, nthreads, world_rank, world_size);
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
