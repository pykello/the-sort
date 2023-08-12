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
#include <set>
#include <sstream>
#include "parallel_partitioned_sort.hpp"
using namespace std;
using namespace std::chrono;

typedef std::function<void(int *, int *, int n)> SortFunc;

struct Options {
    bool printHelp = false;
    int dataSize = 200000000;
    bool runStd = true;
    bool runPartitioned = true;
    bool runMerge = true;
    set<int> threadCounts {2,4,8,12,16,24,32,64,128,256};
};

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

uint64_t measure_min_exec_time(const SortFunc &sort_function, int *data, int *result, int n)
{
    // Runs the sort function 3 times & chooses the lowest time.
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

void run_benchmark_suite(const Options &opts)
{
    int N = opts.dataSize;
    auto result_u = std::make_unique<int[]>(N);
    auto data_u = std::make_unique<int[]>(N);
    auto verification_u = std::make_unique<int[]>(N);
    int *result = result_u.get();
    int *data = data_u.get();
    int *verification = verification_u.get();

    generate(data, N);
    memcpy(verification, data, sizeof(int) * N);
    sort(std::execution::par, verification, verification + N);

#define RUN(name, f) \
    cout << name ": " << measure_min_exec_time(f, data, result, N) << endl; \
    verify(result, verification, N);
#define RUN_MULTITHREADED(name, f, threads) \
    if (opts.threadCounts.count(threads)) {\
        RUN(name "<" #threads ">", f<threads>) \
    }
#define RUN_MULTITHREADED_ALL(name, f) \
    RUN_MULTITHREADED(name, f, 2); \
    RUN_MULTITHREADED(name, f, 4); \
    RUN_MULTITHREADED(name, f, 8); \
    RUN_MULTITHREADED(name, f, 12); \
    RUN_MULTITHREADED(name, f, 16); \
    RUN_MULTITHREADED(name, f, 24); \
    RUN_MULTITHREADED(name, f, 32); \
    RUN_MULTITHREADED(name, f, 64); \
    RUN_MULTITHREADED(name, f, 128); \
    RUN_MULTITHREADED(name, f, 256);

    if (opts.runStd)
    {
        RUN("std seq", stdseqsort);
        RUN("std par", stdparsort);
    }

    if (opts.runPartitioned)
    {
        RUN_MULTITHREADED_ALL("partitioned_sort", parallel_partitioned_sort);
    }
}

int main(int argc, char* argv[])
{
    Options opts;
    for (int i = 1; i < argc; i++) {
        if (strcmp("--disable-std", argv[i]) == 0)
            opts.runStd = false;
        else if (strcmp("--disable-partitioned", argv[i]) == 0)
            opts.runPartitioned = false;
        else if (strcmp("--disable-merge", argv[i]) == 0)
            opts.runMerge = false;
        else if (strcmp("--threads", argv[i]) == 0)
        {
            if (i == argc - 1) {
                opts.printHelp = true;
                std::cerr << "Missing thread counts." << std::endl;
                break;
            }
            std::istringstream tokenStream(argv[i + 1]);
            opts.threadCounts.clear();
            string token;
            while (std::getline(tokenStream, token, ',')) {
                opts.threadCounts.insert(atoi(token.c_str()));
            }
            i++;
        }
        else if (strcmp("--data-size", argv[i]) == 0)
        {
            if (i == argc - 1) {
                opts.printHelp = true;
                std::cerr << "Missing data size." << std::endl;
                break;
            }
            opts.dataSize = atoi(argv[i+1]);
            i++;
        }
        else if (strcmp("--help", argv[i]) == 0)
        {
            opts.printHelp = true;
        }
        else
        {
            std::cerr << "Unknown option " << argv[i] << "." << std::endl;
            opts.printHelp = true;
        }
    }

    if (opts.printHelp)
    {
        cout << argv[0] << " [--disable-std] [--disable-partitioned] [--disable-merge] [--threads t1,t2,t3] [--data-size N]" << endl;
    }
    else
    {
        run_benchmark_suite(opts);
    }

    return 0;
}
