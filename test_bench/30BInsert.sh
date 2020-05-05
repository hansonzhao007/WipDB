echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

NUM=30000000000

rm -rf /mnt/nvm/*
rm -rf /mnt/ssd/*
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
# cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --filllarge=true --hugepage=false --db=/mnt/ssd/kv30B  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandomlarge,stats --logpath=/mnt/nvm --bloom_bits=10 --log=true        --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=1  --print_wa=true | tee kv30B_nvm_hugepage.log 


cgexec -g memory:kv64 ./rocksdb/release/db_bench  --db=/mnt/ssd/rocks30B  --num=$NUM --value_size=100 --batch_size=1000  --benchmarks=fillrandom,stats --wal_dir=/mnt/nvm --bloom_bits=10 --disable_wal=false --cache_size=8388608    --max_background_jobs=7 --open_files=40000 --stats_per_interval=100000000  --stats_interval=100000000 --histogram=true  | tee rocks30B_ssd.log
