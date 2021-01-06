# Introduction

Key-value (KV) stores have become a major storage infrastructure on which databases, file systems, and other data management systems are built. To support efficient indexing and range search, the key-value items must be sorted. However, the operation can be excessively expensive. In the KV systems adopting the popular Log-Structured Merge Tree (LSM) structure or its variants, the write volume can be amplified by tens of times due to its repeated internal merge-sorting operations. 

In this paper we propose a KV store design that leverages relatively stable key distributions to bound the write amplification by a number as low as 4.15 in practice. The key idea is, instead of incrementally sorting KV items in the LSM's hierarchical structure, it writes KV items right in place in an approximately sorted list, much like a bucket sort algorithm does. The design also makes it possible to keep most internal data reorganization operations off the critical paths of read service. The so-called Write-in-place (Wip) scheme has been implemented  with its source code publically available. Experimental results show that WipDB improves write throughput by 3X to 8X (to around 1 Mops/s) over state-of-the-art KV stores.

For more details about WipDB, please refer to IEEE ICDE 2021 paper - "WipDB: A Write-in-place Key-value Store that Mimics Bucket Sort"

# Environment Configuration

1. cmake version 3.10.2
2. install googletest, gflags, snappy

# Compile
1. edit the `config.sh` file, mount the target disks
2. run `build.sh` to compile all the four key-value stores

# Debug

`...can not be used when making a shared object; recompile with -fPIC`
cmake with option `cmake -DBUILD_SHARED_LIBS=ON  ..`

# Run test bench

modify the test bench file in folder `test_bench` and run.

# Other

Folder "kv" contains the source code of WipDB.



