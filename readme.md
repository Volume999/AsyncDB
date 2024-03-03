# Asynchronous Database Access (Async DB)

# Simulation of sequential vs asynchronous operations

## Running the simulation
1. Go to /internal/simulation folder
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