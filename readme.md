# Asynchronous Database Access (Async DB)

# Simulation of sequential vs asynchronous operations

## Running the simulation
1. Go to simulation folder
2. For latency, run in terminal:
    ```bash
    go test -bench=. -benchmem
    ```
    This benchmark runs for a non-specified amount of time (until the benchmark is stable). To run for a specific amount of time, run in terminal:
    ```bash
    go test -bench=. -benchmem -benchtime=100x
    ```
3. For throughput, run in terminal:
    ```bash
    go test -bench=. -benchmem -benchtime=10s
    ```
    The simulation will run for 10 seconds and output the results.
4. To run a complete benchmark:
    ```bash
    go test -bench=. -benchtime=10s -cpu=8 -timeout=0 -benchmem
    ```
    This will run the benchmark for 10 seconds, using 8 CPUs, with no timeout and memory allocation statistics.