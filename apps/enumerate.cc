#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mpi.h>
#include "Peregrine.hh"


bool is_directory(const std::string &path)
{
   struct stat statbuf;
   if (stat(path.c_str(), &statbuf) != 0)
       return 0;
   return S_ISDIR(statbuf.st_mode);
}

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    std::cerr << "USAGE: " << argv[0] << " <data graph> <pattern | #-motifs | #-clique> [# threads]" << std::endl;
    return -1;
  }

  const std::string data_graph_name(argv[1]);
  const std::string pattern_name(argv[2]);
  size_t nthreads = argc < 4 ? 1 : std::stoi(argv[3]);

  int world_rank, world_size;
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  printf("Hello world from process %d, of %d. Thread num: %ld \n", world_rank, world_size, nthreads);


  std::vector<Peregrine::SmallGraph> patterns;
  if (auto end = pattern_name.rfind("motifs"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end-1));
    patterns = Peregrine::PatternGenerator::all(k,
        Peregrine::PatternGenerator::VERTEX_BASED,
        Peregrine::PatternGenerator::INCLUDE_ANTI_EDGES);
  }
  else if (auto end = pattern_name.rfind("clique"); end != std::string::npos)
  {
    auto k = std::stoul(pattern_name.substr(0, end-1));
    patterns.emplace_back(Peregrine::PatternGenerator::clique(k));
  }
  else
  {
    patterns.emplace_back(pattern_name);
  }

  const auto process = [](auto &&a, auto &&cm) { a.map(cm.pattern, 1); };
  std::vector<std::pair<Peregrine::SmallGraph, uint64_t>> result;
  if (is_directory(data_graph_name))
  {
    result = Peregrine::match<Peregrine::Pattern, uint64_t, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(data_graph_name, patterns, nthreads, process, world_rank, world_size);
  }
  else
  {
    Peregrine::SmallGraph G(data_graph_name);
    result = Peregrine::match<Peregrine::Pattern, uint64_t, Peregrine::AT_THE_END, Peregrine::UNSTOPPABLE>(G, patterns, nthreads, process, world_rank, world_size);
  }

  for (const auto &[p, v] : result)
  {
    std::cout << p << ": " << v << std::endl;
  }

  printf("DONE PROCESS %d\n");

  MPI_Finalize();

  return 0;
}
