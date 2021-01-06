echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

NUM=4000000000
LOW_THREAD=3
HIGH_THREAD=3
BLOOMBITS=10


rm -rf /mnt/nvm/*
rm  /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --hugepage=true --db=/mnt/nvm/kv8B  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandom4,stats --logpath=/mnt/nvm --bloom_bits=$BLOOMBITS --log=false        --cache_size=8388608 --low_pool=$LOW_THREAD --high_pool=$HIGH_THREAD --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=2  --print_wa=true --printBucket=true | tee kv_fillrandom4_1.log 


rm -rf /mnt/nvm/*
rm  /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --hugepage=true --db=/mnt/nvm/kv8B  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandom4,stats --logpath=/mnt/nvm --bloom_bits=$BLOOMBITS --log=false        --cache_size=8388608 --low_pool=$LOW_THREAD --high_pool=$HIGH_THREAD --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=2  --print_wa=true --printBucket=true | tee kv_fillrandom4_2.log 


# rm -rf /mnt/nvm/*
# rm  /mnt/ssd/*.log
# echo fb0-=0-= | sudo -S fstrim /mnt/ssd
# echo fb0-=0-= | sudo -S fstrim /mnt/nvm
# echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
# cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --hugepage=true --db=/mnt/nvm/kv8B  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandom4,stats --logpath=/mnt/nvm --bloom_bits=$BLOOMBITS --log=false        --cache_size=8388608 --low_pool=$LOW_THREAD --high_pool=$HIGH_THREAD --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=2  --print_wa=true --printBucket=true | tee kv_fillrandom4_3.log 


# rm -rf /mnt/nvm/*
# rm  /mnt/ssd/*.log
# echo fb0-=0-= | sudo -S fstrim /mnt/ssd
# echo fb0-=0-= | sudo -S fstrim /mnt/nvm
# echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
# cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --hugepage=true --db=/mnt/nvm/kv8B  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandom4,stats --logpath=/mnt/nvm --bloom_bits=$BLOOMBITS --log=false        --cache_size=8388608 --low_pool=$LOW_THREAD --high_pool=$HIGH_THREAD --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=2  --print_wa=true --printBucket=true | tee kv_fillrandom4_4.log 


# rm -rf /mnt/nvm/*
# rm  /mnt/ssd/*.log
# echo fb0-=0-= | sudo -S fstrim /mnt/ssd
# echo fb0-=0-= | sudo -S fstrim /mnt/nvm
# echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
# cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --hugepage=true --db=/mnt/nvm/kv8B  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandom4,stats --logpath=/mnt/nvm --bloom_bits=$BLOOMBITS --log=false        --cache_size=8388608 --low_pool=$LOW_THREAD --high_pool=$HIGH_THREAD --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=2  --print_wa=true --printBucket=true | tee kv_fillrandom4_5.log 

