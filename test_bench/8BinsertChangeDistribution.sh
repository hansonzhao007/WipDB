echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --hugepage=true --db=/mnt/nvm/kv8B  --num=8000000000 --value_size=100 --batch_size=1000 --benchmarks=fillrandom2,stats --logpath=/mnt/ssd --bloom_bits=10 --log=true        --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=100  --print_wa=true | tee kv8B_nvm_hugepage_splitwhen3B.log 
