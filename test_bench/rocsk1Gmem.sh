echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 40000

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'

cgexec -g memory:kv64 ./rocksdb/release/db_bench --db_write_buffer_size=1073741824 --write_buffer_size=1073741824 --db=/mnt/nvm/rocks8B1Gmem  --num=8000000000 --value_size=100 --batch_size=1000  --benchmarks=fillrandom,stats --wal_dir=/mnt/ssd --bloom_bits=10 --disable_wal=true --cache_size=8388608    --max_background_jobs=7 --open_files=40000 --stats_per_interval=100000000  --stats_interval=100000000 --histogram=true  | tee rocks8B_nvm_1GBMem.log
