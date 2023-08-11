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

void stdsort(int *data, int *result, int n)
{
    memcpy(result, data, n * sizeof(int));
    sort(std::execution::seq, result, result + n);
}

void parallel_quicksort(int *data, int *result, int n, int threads)
{
    const int S = 5000;
    const int MAX_THREADS = 256;
    unsigned char *partition = new unsigned char[n];
    
    // calculate pivots
    int sample[S];
    for (int i = 0; i < S; i++)
        sample[i] = data[rand() % n];
    sort(sample, sample + S);
    int pivots[MAX_THREADS];
    for (int i = 0; i < threads; i++)
        pivots[i] = sample[((int64_t)(i + 1) * S) / threads - 1];

    // positional ranges
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

    // count items that will be moved to each parition
    int cnt[MAX_THREADS][MAX_THREADS] = {0}; // cnt[from][to]
    vector<future<void>> vf1;
    for (int i = 0; i < threads; i++)
    {
        vf1.push_back(async(std::launch::async, [&](int tidx) {
            for (int i = pos_from[tidx]; i < pos_to[tidx]; i++) {
                partition[i] = threads - 1;
                while (partition[i] > 0 && data[i] <= pivots[partition[i] - 1])
                    partition[i]--;
                cnt[tidx][partition[i]]++;
            }
        }, i));
    }
    for (auto & f: vf1)
        f.wait();
    
    int offsets[MAX_THREADS][MAX_THREADS] = {0};
    for (int j = 0; j < threads; j++) {
        for (int i = 0; i < threads; i++) {
            if (i != 0)
                offsets[i][j] = offsets[i - 1][j] + cnt[i - 1][j];
            else if (j != 0)
                offsets[i][j] = offsets[threads - 1][j - 1] + cnt[threads - 1][j - 1];
        }
    }

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

    int partition_size[MAX_THREADS] = {0};
    for (int i = 0; i < threads; i++)
        for (int j = 0; j < threads; j++)
            partition_size[j] += cnt[i][j];

    vector<future<void>> vf3;
    int offset = 0;
    for (int i = 0; i < threads; i++)
    {
        vf3.push_back(async(std::launch::async, [&](int from, int size) {
            sort(result + from, result + from + size);
        }, offset, partition_size[i]));
        offset += partition_size[i];
    }
    for (auto & f: vf3)
        f.wait();
}

void parallel_quicksort_2(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 2);
}

void parallel_quicksort_4(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 4);
}

void parallel_quicksort_8(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 8);
}

void parallel_quicksort_16(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 16);
}

void parallel_quicksort_32(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 32);
}

void parallel_quicksort_64(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 64);
}

void parallel_quicksort_128(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 128);
}

void parallel_quicksort_256(int *data, int *result, int n)
{
    parallel_quicksort(data, result, n, 256);
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
    int *result = new int[N];
    int *data = new int[N];
    int *verification = new int[N];

    generate(data, N);
    memcpy(verification, data, sizeof(int) * N);
    sort(verification, verification + N);

    cout << "std: " << benchmark(stdsort, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_2: " << benchmark(parallel_quicksort_2, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_4: " << benchmark(parallel_quicksort_4, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_8: " << benchmark(parallel_quicksort_8, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_16: " << benchmark(parallel_quicksort_16, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_32: " << benchmark(parallel_quicksort_32, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_64: " << benchmark(parallel_quicksort_64, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_128: " << benchmark(parallel_quicksort_128, data, result, N) << endl;
    verify(result, verification, N);
    cout << "pqsort_256: " << benchmark(parallel_quicksort_256, data, result, N) << endl;
    verify(result, verification, N);
    return 0;
}
