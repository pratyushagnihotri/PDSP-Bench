<h2 align="center">
  Experiment, Analysis, and Benchmark on PDSP-Bench for DSPS
</h2>

PDSP-Bench is a benchmarking system for **parallel and distributed stream processing** on **heterogeneous hardware configurations**. It supports deploying and benchmarking different **Stream Processing Engines (SPEs)** as the **System Under Test (SUT)**—including **Apache Flink** and **Apache Storm**—using real-world and synthetic streaming applications. PDSP-Bench collects and visualizes performance metrics such as **end-to-end latency**, **throughput**, and **resource utilization**, and exports experiment artifacts for offline analysis and ML-based modeling.


## Citation

Please cite our papers, if you find this work useful or use it in your paper as a baseline.

```
@inproceedings{Agnihotri_TPCTC_2024_PDSPBench,
author    =       {Agnihotri, Pratyush, and Koldehofe, Boris, and Heinrich, Roman, and Binnig, Carsten and Luthra, Manisha},
editor    =       {Nambiar, Raghunath and Poess, Meikel},
title     =       {PDSP-Bench: A Benchmarking System for¬†Parallel and¬†Distributed Stream Processing},
booktitle =       {Performance Evaluation and Benchmarking},
year      =       {2026},
publisher =       {Springer Nature Switzerland},
pages     =       {1--23}
}

@inproceedings{Agnihotri_SIGMOD_Demo_PDSPBench_2025,
author      =     {Agnihotri, Pratyush and Binnig, Carsten},
title       =     {Demonstrating PDSP-Bench: A Benchmarking System for Parallel and Distributed Stream Processing},
year        =     {2025},
booktitle   =     {Companion of the 2025 International Conference on Management of Data},
pages       =     {7–10},
numpages    =     {4},
}

```

<h3>Dedicated Repository for Paper Submission:</h3>

This repository supports our paper submission  **Benchmarking Parallel Stream Processing in Heterogeneous Environments: An Empirical Analysis with PDSP-Bench** and contains the main components required to provision infrastructure, deploy stream processing engines, run benchmarking experiments, and analyze results.

## Key Concepts

- SUT (System Under Test): The stream processing engine being benchmarked (e.g., Flink, Storm).
- PQP (Parallel Query Plan): A parallelization configuration for an application/topology (e.g., per-operator DoP vector).
- Workload Configuration: Stream parameters such as event rate, query parameters, execution duration, iterations, etc.
- Heterogeneous Hardware: Clusters where nodes differ in CPU, memory, storage, or accelerators.

## Supported Stream Processing Engines (SUTs)

PDSP-Bench is designed to benchmark multiple SPEs under a unified workflow.

### Apache Flink

- **Cluster roles:** JobManager + TaskManagers

- **Parallelism mapping:** PQP/DoP settings map to Flink operator/subtask parallelism

- **Experiment lifecycle:** deploy cluster → submit job → run for duration → collect metrics → analyze/export

### Apache Storm

- **Cluster roles:** ZooKeeper + Nimbus + Supervisors

- **Parallelism mapping:** PQP/DoP settings map to spout/bolt parallelism (executors/tasks)

- **Experiment lifecycle:** deploy cluster → submit topology → run for duration → collect metrics → analyze/export

## Workloads

PDSP-Bench supports benchmarking using:

- 14 real-world applications

Applications can be executed under different PQPs, workload settings, and resource configurations.

## Repository Structure

- [pdsp-bench_Cloud_setup:](https://github.com/pratyushagnihotri/PDSP-Bench/tree/main/pdsp-bench_Cloud_setup#readme)
CloudLab setup instructions and scripts to provision resources and install dependencies.

- [pdsp-bench_controller:](https://github.com/pratyushagnihotri/PDSP-Bench/tree/main/pdsp-bench_controller#readme)
  - Backend controller. Provides APIs to automate:

  - cluster creation and SUT deployment (Flink/Storm)

  - job/topology submission and experiment lifecycle

  - persistence of metadata and results (e.g., SQLite/MongoDB depending on configuration)

- [pdsp-bench_wui:](https://github.com/pratyushagnihotri/PDSP-Bench/tree/main/pdsp-bench_wui#readme)
Web UI for provisioning inputs, workload configuration, experiment execution, and visualization.

- [pdsp-bench_benchmark_data](https://github.com/pratyushagnihotri/PDSP-Bench/tree/main/pdsp-bench_benchmark_data)
Selected sample experiment outputs (real-world and synthetic applications).

> Note: Detailed steps about each component of PDSP-Bench is provided in their respective README.md files.

## PDSP-Bench Workflow (High-Level)

1. Provision CloudLab resources (cluster nodes).
1. Add node hostnames in the Web UI.
1. Create a cluster and deploy an SUT (Flink or Storm).
1. Select an application (real-world or synthetic).
1. Configure PQP/parallelism and workload parameters (event rate, duration, iterations, enumeration strategy).
1. Run the experiment.
1. Monitor real-time performance metrics.
1. Analyze historical results and export artifacts for offline analysis or ML.

## Step-by-Step Operations

>  Detailed instructions for each component are provided in the component-level README files.
1. Create a **CloudLab** account and set up a cluster using [pdsp-bench_Cloud_setup/](https://github.com/pratyushagnihotri/PDSP-Bench/tree/main/pdsp-bench_Cloud_setup#readme).
2. Clone the repository into your home directory.
3. Set up and start [pdsp-bench_controller/:](https://github.com/pratyushagnihotri/PDSP-Bench/tree/main/pdsp-bench_controller#readme).
4. Set up and start [pdsp-bench_wui/](https://github.com/pratyushagnihotri/PDSP-Bench/tree/main/pdsp-bench_wui#readme).
5. In the WUI, open Explore Node and add hostnames from all CloudLab nodes.
6. Go to **Create Cluster** and select:
    - number of nodes
    - node allocation / roles
    - SUT: **Apache Flink** or **Apache Storm**
7. Deploy the selected SUT:
    - Flink: JobManager + TaskManagers
    - Storm: ZooKeeper + Nimbus + Supervisors
8. Select the cluster and configure the experiment:
    - application/topology
    - event rate
    - parallelism / PQP / DoP vector
    - execution time
    - iterations
    - enumeration strategy
9. Run the experiment and monitor:
    - end-to-end latency
    - throughput
    - resource utilization
10. Download/export results:
    - configurations and metrics as JSON
    - optional graph representation artifacts
    - persisted records in configured databases (e.g., MongoDB/SQLite)

## Outputs and Artifacts

Each experiment run typically produces:

- run metadata (SUT, nodes, versions, workload parameters, PQP settings)

- time-series performance metrics (throughput, latency, utilization)

- exported JSON artifacts for offline analysis

- optional PQP/topology graph representations

- persisted results in configured databases (depending on configuration)

## Troubleshooting

- **Deployment succeeds but jobs/topologies fail:** verify all required services are running on the correct nodes (Flink JM/TM; Storm ZK/Nimbus/Supervisors).

- **Throughput is capped:** confirm PQP/parallelism does not exceed available slots/executors and verify the source generator configuration.

- **Metrics missing in UI:** confirm controller database connectivity and metric collectors are enabled.