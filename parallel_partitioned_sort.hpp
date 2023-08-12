#pragma once

#include <algorithm>
#include <cstdlib>
#include <vector>
#include <future>

template<int threads>
void parallel_partitioned_sort(int *data, int *result, int n)
{
    const int S = 5000;
    const int MAX_THREADS = 256;
    auto partition = std::make_unique<unsigned char[]>(n);
    
    /*
     * Calculate Pivots
     * To do this, we choose a small sample of the data, and use that to choose
     * pivots. This will give us more equi-distance pivots than just choosing
     * random ones.
     */
    int sample[S];
    for (int i = 0; i < S; i++)
        sample[i] = data[rand() % n];
    std::sort(sample, sample + S);
    int pivots[threads];
    for (int i = 0; i < threads; i++)
        pivots[i] = sample[((int64_t)(i + 1) * S) / threads - 1];

    /*
     * Calculate which positional ranges of array each thread is responsible for.
     */
    int pos_from[threads], pos_to[threads];
    for (int i = 0; i < threads; i++)
    {
        if (i)
            pos_from[i] = pos_to[i - 1];
        else
            pos_from[i] = 0;
        if (i == threads - 1)
            pos_to[i] = n;
        else
            pos_to[i] = ((int64_t)(i + 1) * n) / threads;
    }

    /*
     * Count items that will be moved from each partion "x" to partition "y"
     * and put it to cnt[x][y].
     * 
     * While doing this, also fill partition[i] says which partition data[i]
     * belongs to.
     * 
     * We do this completely parallel.
     */
    int cnt[threads][threads] = {0}; // cnt[from][to]
    std::vector<std::future<void>> vf1;
    for (int i = 0; i < threads; i++)
    {
        vf1.push_back(std::async(std::launch::async, [&](int tidx) {
            for (int i = pos_from[tidx]; i < pos_to[tidx]; i++) {
                partition[i] = threads - 1;
                // this could also be binary search, but for small sizes probably
                // it doesn't matter much. Maybe upto 64. Need to improve.
                while (partition[i] > 0 && data[i] <= pivots[partition[i] - 1])
                    partition[i]--;
                cnt[tidx][partition[i]]++;
            }
        }, i));
    }
    for (auto & f: vf1)
        f.wait();

    /*
     * Create offsets array that will be useful when moving items between partitions.
     * offsets[x][y] is the next position that an item from partition x to partition y
     * should be placed at.
     *
     * Time doesn't depend on N, so no need to parallelize.
     */
    int offsets[threads][threads] = {0};
    for (int j = 0; j < threads; j++) {
        for (int i = 0; i < threads; i++) {
            if (i != 0)
                offsets[i][j] = offsets[i - 1][j] + cnt[i - 1][j];
            else if (j != 0)
                offsets[i][j] = offsets[threads - 1][j - 1] + cnt[threads - 1][j - 1];
        }
    }

    /*
     * Move data between partitions. Completely parallel.
     */
    std::vector<std::future<void>> vf2;
    for (int i = 0; i < threads; i++)
    {
        vf2.push_back(std::async(std::launch::async, [&](int tidx) {
            for (int j = pos_from[tidx]; j < pos_to[tidx]; j++) {
                int p = partition[j];
                result[offsets[tidx][p]++] = data[j];
            }
        }, i));
    }
    for (auto & f: vf2)
        f.wait();

    /* Calculate size of each partition */
    int partition_size[threads] = {0};
    for (int i = 0; i < threads; i++)
        for (int j = 0; j < threads; j++)
            partition_size[j] += cnt[i][j];

    /*
     * Sort each partition. Completely parallel.
     */
    std::vector<std::future<void>> vf3;
    int offset = 0;
    for (int i = 0; i < threads; i++)
    {
        vf3.push_back(std::async(std::launch::async, [&](int from, int size) {
            // now that we have partitioned the data we can use the plain old sequential std sort
            std::sort(std::execution::seq, result + from, result + from + size);
        }, offset, partition_size[i]));
        offset += partition_size[i];
    }
    for (auto & f: vf3)
        f.wait();
}
