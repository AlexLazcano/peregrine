ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
LDFLAGS=-L/usr/local/lib -lpthread -latomic -Ltbb2020/lib/intel64/gcc4.8 -ltbb
BLISS_LDFLAGS=-L$(ROOT_DIR)/core/bliss-0.73/ -lbliss
CFLAGS=-O3 -g -std=c++2a -Wall -Wextra -Wpedantic -fPIC -fconcepts -I$(ROOT_DIR)/core/ -Itbb2020/include
OBJ=core/DataGraph.o core/PO.o core/utils.o core/PatternGenerator.o $(ROOT_DIR)/core/showg.o
OUTDIR=bin/
CC=g++-10
MPICC=mpic++

all: bliss fsm count test existence-query convert_data

core/roaring.o: core/roaring/roaring.c
	gcc -c core/roaring/roaring.c -o $@ -O3 -Wall -Wextra -Wpedantic -fPIC 

%.o: %.cc
	$(CC) -c $? -o $@ $(CFLAGS)

fsm: apps/fsm.cc $(OBJ) core/roaring.o bliss
	$(CC) apps/fsm.cc $(OBJ) core/roaring.o -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

existence-query: apps/existence-query.cc $(OBJ) bliss
	$(CC) apps/existence-query.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

enumerate: apps/enumerate.cc $(OBJ) bliss
	$(CC) apps/enumerate.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

enumMPI: apps/enumerate.cc $(OBJ) bliss
	$(MPICC) apps/enumerate.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

match: apps/match.cc $(OBJ) bliss
	$(MPICC) apps/match.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

count: apps/count.cc $(OBJ) bliss
	$(MPICC) apps/count.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

countMPI: apps/count.cc $(OBJ) bliss
	$(MPICC) apps/count.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

testMPI: apps/testMPI.cc $(OBJ) bliss
	$(MPICC) apps/testMPI.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

range: apps/range.cc $(OBJ) bliss
	$(MPICC) apps/range.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

steal: apps/steal.cc $(OBJ) bliss
	$(MPICC) apps/steal.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

bcast: apps/bcast.cc $(OBJ) bliss
	$(MPICC) apps/bcast.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

scatter: apps/scatter.cc $(OBJ) bliss
	$(MPICC) apps/scatter.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

output: apps/output.cc $(OBJ) bliss
	$(CC) apps/output.cc $(OBJ) -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) $(CFLAGS)

test: core/test.cc $(OBJ) core/DataConverter.o core/roaring.o bliss
	$(CC) core/test.cc -DTESTING $(OBJ) core/DataConverter.o core/roaring.o -o $(OUTDIR)/$@ $(BLISS_LDFLAGS) $(LDFLAGS) -lUnitTest++ $(CFLAGS)

convert_data: core/convert_data.cc core/DataConverter.o core/utils.o
	$(CC) -o $(OUTDIR)/$@ $? $(LDFLAGS) $(CFLAGS)

bliss:
	make -C ./core/bliss-0.73

clean:
	make -C ./core/bliss-0.73 clean
	rm -f core/*.o bin/*

runCount: 
	mpirun -np 4 bin/countMPI data/citeseer 4-motifs 1

runTest: 
	mpirun -np 3 bin/testMPI