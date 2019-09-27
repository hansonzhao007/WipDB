echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

iostat -x d 10 nvme0n1 >> io.log &
while true
do
mpstat -P ALL >> cpu.log
done &

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
rm io.log
rm cpu.log
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --hugepage=true --db=/mnt/nvm/kv8B  --num=8000000000 --value_size=100 --batch_size=1000 --benchmarks=fillrandom,stats --logpath=/mnt/ssd --bloom_bits=10 --log=true        --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true  --partition=100  --print_wa=true | tee kv8B_nvm_hugepage.log 
cp io.log io_wip.log
cp cpu.log cpu_wip.log

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
rm io.log
cgexec -g memory:kv64 ./rocksdb/release/db_bench  --db=/mnt/nvm/rocks8B  --num=8000000000 --value_size=100 --batch_size=1000  --benchmarks=fillrandom,stats --wal_dir=/mnt/ssd --bloom_bits=10 --disable_wal=false --cache_size=8388608    --max_background_jobs=7 --open_files=40000 --stats_per_interval=100000000  --stats_interval=100000000 --histogram=true  | tee rocks8B_nvm.log
cp io.log io_rocks.log

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
rm io.log
cgexec -g memory:kv64 ./pebblesdb/release/db_bench  --db=/mnt/nvm/peb8B  --num=8000000000 --value_size=100 --batch_size=1000  --benchmarks=fillrandom,stats --logpath=/mnt/ssd --bloom_bits=10 --log=true       --cache_size=8388608     --bg_threads=6         --open_files=800000 --stats_interval=100000000 --histogram=true --compression=false --write_buffer_size=67108864   | tee peb8B_nvm.log
cp io.log io_pebbles.log

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
rm io.log
cgexec -g memory:kv64 ./leveldb/release/db_bench  --db=/mnt/nvm/level8B  --num=8000000000 --value_size=100 --batch_size=1000 --benchmarks=fillrandom,stats --logpath=/mnt/ssd --bloom_bits=10 --log=1       --cache_size=8388608                            --open_files=40000  --stats_interval=100000000  --histogram=1                        --write_buffer_size=67108864  --max_file_size=67108864   --print_wa=true | tee level8B_nvm.log
cp io.log io_level.log

exit
