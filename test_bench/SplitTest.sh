echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

rm -rf /mnt/nvm/*

echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv1B --batch_size=1000 --num=1000000000 --value_size=100 --benchmarks=fillrandom,overwrite,stats --logpath=/mnt/nvm --bloom_bits=10 --log=false        --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=10000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=1  | tee kv1BLoad.log 

