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

# More
WipDB's implementation is in folder `kv`.


