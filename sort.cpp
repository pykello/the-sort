#include <iostream>
#include <map>
#include <cstdlib>
#include <execution>
#include <cstring>
#include <algorithm>
#include <functional>
#include <iomanip>
#include <future>
#include <chrono>
using namespace std;
using namespace std::chrono;

uint64_t MeasureDurationMs(const std::function<void()> &body)
{
    auto start = high_resolution_clock::now();

    body();

    auto stop = high_resolution_clock::now();
    auto duration  = duration_cast<microseconds>(stop - start);
    return duration.count() / 1000;
}

void generate(int *data, int n)
{
    for (int i = 0; i < n; i++)
        data[i] = rand();
}

void stdseqsort(int *data, int *result, int n)
{
    memcpy(result, data, n * sizeof(int));
    sort(std::execution::seq, result, result + n);
}

void stdparsort(int *data, int *result, int n)
{
    memcpy(result, data, n * sizeof(int));
    sort(std::execution::par, result, result + n);
}

void parallel_partitioned_sort(int *data, int *result, int n, int threads)
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
    sort(sample, sample + S);
    int pivots[MAX_THREADS];
    for (int i = 0; i < threads; i++)
        pivots[i] = sample[((int64_t)(i + 1) * S) / threads - 1];

    /*
     * Calculate which positional ranges of array each thread is responsible for.
     */
    int pos_from[MAX_THREADS], pos_to[MAX_THREADS];
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
    int cnt[MAX_THREADS][MAX_THREADS] = {0}; // cnt[from][to]
    vector<future<void>> vf1;
    for (int i = 0; i < threads; i++)
    {
        vf1.push_back(async(std::launch::async, [&](int tidx) {
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
    int offsets[MAX_THREADS][MAX_THREADS] = {0};
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
    vector<future<void>> vf2;
    for (int i = 0; i < threads; i++)
    {
        vf2.push_back(async(std::launch::async, [&](int tidx) {
            for (int j = pos_from[tidx]; j < pos_to[tidx]; j++) {
                int p = partition[j];
                result[offsets[tidx][p]++] = data[j];
            }
        }, i));
    }
    for (auto & f: vf2)
        f.wait();

    /* Calculate size of each partition */
    int partition_size[MAX_THREADS] = {0};
    for (int i = 0; i < threads; i++)
        for (int j = 0; j < threads; j++)
            partition_size[j] += cnt[i][j];

    /*
     * Sort each partition. Completely parallel.
     */
    vector<future<void>> vf3;
    int offset = 0;
    for (int i = 0; i < threads; i++)
    {
        vf3.push_back(async(std::launch::async, [&](int from, int size) {
            // now that we have partitioned the data we can use the plain old sequential std sort
            sort(std::execution::seq, result + from, result + from + size);
        }, offset, partition_size[i]));
        offset += partition_size[i];
    }
    for (auto & f: vf3)
        f.wait();
}

void parallel_partitioned_sort_2(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 2);
}

void parallel_partitioned_sort_4(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 4);
}

void parallel_partitioned_sort_8(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 8);
}

void parallel_partitioned_sort_12(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 12);
}

void parallel_partitioned_sort_16(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 16);
}

void parallel_partitioned_sort_24(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 24);
}

void parallel_partitioned_sort_32(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 32);
}

void parallel_partitioned_sort_64(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 64);
}

void parallel_partitioned_sort_128(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 128);
}

void parallel_partitioned_sort_256(int *data, int *result, int n)
{
    parallel_partitioned_sort(data, result, n, 256);
}

uint64_t benchmark(const std::function<void(int *, int *, int n)> &sort_function, int *data, int *result, int n)
{
    uint64_t min_time = UINT64_MAX;
    int repeats = 3;
    for (int i = 0; i < repeats; i++)
    {
        memset(result, 0, sizeof(int) * n);
        min_time = min(MeasureDurationMs([&]() {
            sort_function(data, result, n);
        }), min_time);
    }

    return min_time;
}

void verify(int *result, int *verification, int n)
{
    for (int i = 0; i < n; i++)
        if (result[i] != verification[i])
        {
            cout << "Verification failed, result[" << i << "]=" << result[i] << ", but expected: " << verification[i] << endl;
            return;
        }
    cout << "Verified successfully!" << endl;
}

int main()
{
    const int N = 200000000;
    auto result_u = std::make_unique<int[]>(N);
    auto data_u = std::make_unique<int[]>(N);
    auto verification_u = std::make_unique<int[]>(N);
    int *result = result_u.get();
    int *data = data_u.get();
    int *verification = verification_u.get();

    generate(data, N);
    memcpy(verification, data, sizeof(int) * N);
    sort(verification, verification + N);

    cout << "std seq: " << benchmark(stdseqsort, data, result, N) << endl;
    verify(result, verification, N);
    cout << "std par: " << benchmark(stdparsort, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_2: " << benchmark(parallel_partitioned_sort_2, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_4: " << benchmark(parallel_partitioned_sort_4, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_8: " << benchmark(parallel_partitioned_sort_8, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_12: " << benchmark(parallel_partitioned_sort_12, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_16: " << benchmark(parallel_partitioned_sort_16, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_24: " << benchmark(parallel_partitioned_sort_24, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_32: " << benchmark(parallel_partitioned_sort_32, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_64: " << benchmark(parallel_partitioned_sort_64, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_128: " << benchmark(parallel_partitioned_sort_128, data, result, N) << endl;
    verify(result, verification, N);
    cout << "partitioned_sort_256: " << benchmark(parallel_partitioned_sort_256, data, result, N) << endl;
    verify(result, verification, N);
    return 0;
}
