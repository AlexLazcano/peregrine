CC=mpic++
# MPI_Compile = $(shell mpicc --showme:compile)
MPI_Compile = -lmpi


all: hello

hello:
	$(CC) $(MPI_Compile) mpi.cc -o hello.exe --showme

clean: 
	rm *.exe
	