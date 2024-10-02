# ACGraph

## Build
ACGraph use CMake as its build system. Follow the steps below to build the project.

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```

## Graph Preprocessing
ACGraph use an optimized hybrid graph format to store the graph. We provide a tool to convert csr format to ACGraph format, where the csr graph consists of two files: `offset` (the offset of each vertex, represented by an 8-byte unsigned integer array of size $|V|+1$) and `data` (the adjacency list of each vertex, represented by an 4-byte unsigned integer array of size $|E|$).

To convert, run the following command in the `build` directory:
```bash
./formatter <input_path> <output_path> [--deg_bound <degree_threshold>]
```

We also provide a tool to convert text edge list to csr format, where the edge list is represented by a file with each line containing two vertex ids, for example:
```
1 2
1 3
2 3
```

To convert, run the following command in the `build` directory:
```bash
./el2csr <input_path> <output_path>
```

## Run

To run the ACGraph, run the following command in the `build` directory:
```bash
./<algorithm_name> <input_path> [-tn <thread_num>] [-cmb <buffer_pool_size, MB>]
```

For example, to run BFS, run the following command in the `build` directory:
```bash
./bfs <input_path> -tn 16 -cmb 128 -src 1234
```
which represents that the BFS is executed on the graph stored in `input_path` with 16 threads, 128 MB buffer pool, and the source vertex is 1234.

## Acknowledgement

We would like to thank the following projects:
- [liburing](https://github.com/axboe/liburing)
- [PIGO](https://github.com/GT-TDAlab/PIGO)
- [parallel-hashmap](https://github.com/greg7mdp/parallel-hashmap)
- [concurrentqueue](https://github.com/cameron314/concurrentqueue)
- [Multi Bucket Queue ](https://github.com/mcj-group/cps/tree/main)

