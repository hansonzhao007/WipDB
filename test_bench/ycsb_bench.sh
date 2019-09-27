echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

OPS=8000000
NUM=1500000000

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cp -R /mnt/ssd/kv_ycsb /mnt/nvm/
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --cache_size=8388608 --logpath=/mnt/ssd --db=/mnt/nvm/kv_ycsb   --threads=4 --open_files=40000  --num=$NUM --range=$NUM  --reads=2000000 --partition=100 --write_buffer_size=2097152  --batch_size=1000  --stats_interval=1000000  --bloom_bits=10 --low_pool=3 --high_pool=3 --value_size=100  --direct_io=false --histogram=true    --log=true --skiplistrep=true   --benchmarks=loadreverse,ycsbc,ycsbb,ycsbd,ycsba,ycsbf,ycsbe,stats         --ycsb_ops_num=$OPS --seek_nexts=100  --use_existing_db=true  --print_wa=true  --log_dio=true --hugepage=true | tee kv_ycsb.log


rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cp -R /mnt/ssd/kv_ycsb /mnt/nvm/
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --cache_size=8388608 --logpath=/mnt/ssd --db=/mnt/nvm/kv_ycsb   --threads=4 --open_files=40000  --num=$NUM --range=$NUM  --reads=2000000 --partition=100 --write_buffer_size=2097152  --batch_size=1000  --stats_interval=1000000  --bloom_bits=10 --low_pool=3 --high_pool=3 --value_size=100  --direct_io=false --histogram=true    --log=true --skiplistrep=false   --benchmarks=loadreverse,ycsbc,ycsbb,ycsbd,ycsba,ycsbf,ycsbe,stats         --ycsb_ops_num=$OPS --seek_nexts=100  --use_existing_db=true  --print_wa=true  --log_dio=true --hugepage=true | tee kv_ycsb_hash.log



rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cp -R /mnt/ssd/rocks_ycsb /mnt/nvm/
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./rocksdb/release/db_bench    --cache_size=8388608  --wal_dir=/mnt/ssd --db=/mnt/nvm/rocks_ycsb --threads=4 --open_files=40000  --num=$NUM --range=$NUM  --reads=2000000                                              --batch_size=1000  --stats_interval=1000000  --bloom_bits=10 --max_background_jobs=7    --value_size=100                    --histogram=true  --disable_wal=false              --benchmarks=loadreverse,ycsbc,ycsbb,ycsbd,ycsba,ycsbf,ycsbe,stats,levelstats  --ycsb_ops_num=$OPS  --seek_nexts=100  --use_existing_db=true   --stats_per_interval=1000000 | tee rocks_ycsb.log

rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cp -R /mnt/ssd/peb_ycsb /mnt/nvm/
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./pebblesdb/release/db_bench  --cache_size=8388608 --logpath=/mnt/ssd --db=/mnt/nvm/peb_ycsb  --threads=4 --open_files=40000   --num=$NUM  --range=$NUM  --reads=2000000                                              --batch_size=1000  --stats_interval=1000000  --bloom_bits=10 --bg_threads=6             --value_size=100                    --histogram=true  --log=true                       --benchmarks=loadreverse,ycsbc,ycsbb,ycsbd,ycsba,ycsbf,ycsbe,stats            --ycsb_ops_num=$OPS  --seek_nexts=100  --use_existing_db=true  --write_buffer_size=67108864     | tee peb_ycsb.log


rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cp -R /mnt/ssd/level_ycsb /mnt/nvm/
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./leveldb/release/db_bench    --cache_size=8388608 --logpath=/mnt/ssd --db=/mnt/nvm/level_ycsb --threads=4 --open_files=40000   --num=$NUM --range=$NUM  --reads=2000000                                              --batch_size=1000  --stats_interval=1000000  --bloom_bits=10                            --value_size=100                    --histogram=true   --log=true                      --benchmarks=loadreverse,ycsbc,ycsbb,ycsbd,ycsba,ycsbf,ycsbe,stats            --ycsb_ops_num=$OPS  --seek_nexts=100  --use_existing_db=true  --print_wa=true  --write_buffer_size=67108864  --max_file_size=67108864  | tee level_ycsb.log
