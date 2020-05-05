echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000


OPS=8000000
NUM=1200000000
BLOOM_BITS=16
CACHE=8388608
VALUE=100

rm -rf /mnt/nvm/*
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./pebblesdb/release/db_bench  --db=/mnt/nvm/peb1B --batch_size=1000 --num=$NUM --writes=10000000 --value_size=$VALUE --benchmarks=load,overwrite,stats --logpath=/mnt/nvm --bloom_bits=$BLOOM_BITS --log=false       --cache_size=$CACHE     --bg_threads=6         --open_files=40000 --stats_interval=10000000 --histogram=true --compression=false --write_buffer_size=67108864    | tee peb1B.log

echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./leveldb/release/db_bench  --db=/mnt/nvm/level1B --batch_size=1000 --num=$NUM --writes=10000000 --value_size=$VALUE --benchmarks=load,overwrite,stats --logpath=/mnt/nvm --bloom_bits=$BLOOM_BITS --log=0       --cache_size=$CACHE                                --open_files=40000  --stats_interval=10000000  --histogram=1                        --write_buffer_size=67108864  --max_file_size=67108864   | tee level1B.log

echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv1B --batch_size=1000 --num=$NUM --writes=10000000 --value_size=$VALUE --benchmarks=load,overwrite,stats --logpath=/mnt/nvm --bloom_bits=$BLOOM_BITS --log=false        --cache_size=$CACHE --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=10000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=100  | tee kv1B.log 

echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./rocksdb/release/db_bench  --db=/mnt/nvm/rocks1B --batch_size=1000 --num=$NUM --writes=10000000 --value_size=$VALUE --benchmarks=load,overwrite,stats --wal_dir=/mnt/nvm --bloom_bits=$BLOOM_BITS --disable_wal=true --cache_size=$CACHE  --max_background_jobs=7 --open_files=40000 --stats_interval=10000000 --histogram=true  | tee rocks1B.log
