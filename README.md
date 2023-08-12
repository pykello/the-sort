# Benchmarking parallel sort algorithms!

## Installing Requirements

```
sudo apt update
sudo apt install build-essential cmake libtbb-dev
```

## Building & Running

```
mkdir build
cd build
cmake ..
make

./sort
```

## Results

* Data: 200M random 32-bit integers
* [Ryzen 3900x](https://www.amd.com/en/product/8436): 12 cores, 24 threads
* [EPYC 7502P](https://www.amd.com/en/products/cpu/amd-epyc-7502p): 32 cores, 64 threads

| Algorithm        | threads | Ryzen 3900x | EPYC 7502P |
|------------------|---------|-------------|------------|
| std::sort seq    | 1       | 13.760 s    | 18.520 s   |
| std::sort par    | all     | 1.554 s     | 1.627 s    |
| partitioned_sort | 2       | 7.812 s     | 10.174 s   |
| partitioned sort | 4       | 4.155 s     | 5.255 s    |
| partitioned sort | 8       | 2.322 s     | 2.887 s    |
| partitioned sort | 12      | 1.807 s     | 2.041 s    |
| partitioned sort | 16      | 1.550 s     | 1.617 s    |
| partitioned sort | 24      | **1.215 s** | 1.211 s    |
| partitioned sort | 32      | 1.410 s     | 1.016 s    |
| partitioned sort | 64      | 1.584 s     | **0.808 s**|
| partitioned sort | 128     | 2.201 s     | 1.125 s    |
| partitioned sort | 256     | 3.500 s     | 1.685 s    |

