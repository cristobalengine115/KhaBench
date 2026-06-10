# KhaBench

A distributed benchmarking framework for evaluating multimodel database management systems on resource-constrained hardware.

## Overview

KhaBench compares the performance of three multimodel databases — *ArangoDB, **OrientDB, and **Couchbase* — across graph, document, and key-value workloads. The benchmark is deployed on a *Raspberry Pi 5 cluster* and models a *social commerce network* to simulate real-world query patterns.

Developed as part of a Computer Engineering thesis at the National Autonomous University of Mexico (UNAM).

## Research Questions

- How do multimodel databases perform under distributed, resource-constrained environments?
- How does performance vary across dataset sizes, thread counts, and cache conditions?
- Which system offers the best trade-off between latency, throughput, and resource usage?

## Benchmark Design

| Variable | Values |
|---|---|
| Databases | ArangoDB, OrientDB, Couchbase |
| Dataset sizes | Small (S), Medium (M), Large (L) |
| Thread counts | 1, 2, 4, 8, 16, 32 |
| Cache conditions | Warm, Cold |
| Metrics | Latency (p50, p95, p99), CPU usage, RAM usage, Throughput |

## Infrastructure

- *Hardware:* Raspberry Pi 5 cluster
- *Workload model:* Social commerce network (users, products, orders, reviews, relationships)
- *Benchmark client:* Custom Java implementation

## Results Summary

Performance varied significantly across databases depending on workload type, dataset size, and cache state. Full analysis and figures are available in the thesis document.


## Author

*Edgar Cristóbal García Gutiérrez*  
Computer Engineering, UNAM  
ggcris115@gmail.com

## License

MIT License
