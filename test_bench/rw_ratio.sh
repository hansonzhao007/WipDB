# 3 read thread
# 1 write thread
# The write thread starts after SLEEP seconds, then writes WRITE records. Each read thread will read YCSB_OPS records.

SLEEP=200
WRITE=10000000
YCSB_OPS=10000000
REPORT_INTERVAL=20
THREADS=4
BLOOMBITS=16
NUM=1000000000

echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

rm -rf /mnt/nvm/*

rm /mnt/nvm/*.log
rm /mnt/ssd/*.log
rm -rf /mnt/nvm/kv1B
cp -R /mnt/ssd/kv1B /mnt/nvm/kv1B
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --report_interval=$REPORT_INTERVAL --writes=$WRITE --rwdelay=4 --db=/mnt/nvm/kv1B --logpath=/mnt/ssd --threads=$THREADS --open_files=200000  --range=$NUM --num=$NUM --reads=1000000 --bloom_bits=$BLOOMBITS --low_pool=2 --high_pool=2  --stats_interval=1000000 --value_size=100  --benchmarks=r100,r75,r50,r25,r0,stats        --ycsb_ops_num=$YCSB_OPS   --use_existing_db=true --histogram=true --log=false --write_buffer_size=2097152 --skiplistrep=false --sleep=$SLEEP --print_wa=false --log_dio=true  --hugepage=true | tee kv_rw_ratio.log                  


rm /mnt/nvm/*.log
rm /mnt/ssd/*.log
rm -rf /mnt/nvm/peb1B
cp -R /mnt/ssd/peb1B /mnt/nvm/peb1B
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./pebblesdb/release/db_bench   --report_interval=$REPORT_INTERVAL --writes=$WRITE --rwdelay=2 --db=/mnt/nvm/peb1B --logpath=/mnt/ssd --threads=$THREADS --open_files=200000 --range=$NUM --num=$NUM --reads=1000000 --bloom_bits=$BLOOMBITS --bg_threads=3              --stats_interval=1000000 --value_size=100  --benchmarks=r100,r75,r50,r25,r0,stats          --ycsb_ops_num=$YCSB_OPS  --use_existing_db=true --histogram=true    --log=false --write_buffer_size=67108864  --sleep=$SLEEP | tee peb_rw_ratio.log


rm /mnt/nvm/*.log
rm /mnt/ssd/*.log
rm -rf /mnt/nvm/rocks1B
cp -R /mnt/ssd/rocks1B /mnt/nvm/rocks1B
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./rocksdb/release/db_bench --stats_interval_seconds=$REPORT_INTERVAL --writes=$WRITE --rwdelay=4 --db=/mnt/nvm/rocks1B --wal_dir=/mnt/ssd --threads=$THREADS --open_files=200000  --range=$NUM --num=$NUM --reads=1000000 --bloom_bits=$BLOOMBITS    --max_background_jobs=4  --stats_interval=1000000 --value_size=100  --benchmarks=r100,r75,r50,r25,r0,stats,levelstats --ycsb_ops_num=$YCSB_OPS  --use_existing_db=true --histogram=true --disable_wal=true  --sleep=$SLEEP --stats_per_interval=1000000  --stats_interval=1000000 | tee rocks_rw_ratio.log

rm /mnt/nvm/*.log
rm /mnt/ssd/*.log
rm -rf /mnt/nvm/level1B
cp -R /mnt/ssd/level1B /mnt/nvm/level1B
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'
cgexec -g memory:kv64 ./leveldb/release/db_bench  --report_interval=$REPORT_INTERVAL  --writes=$WRITE --rwdelay=4 --db=/mnt/nvm/level1B --logpath=/mnt/ssd --threads=$THREADS --open_files=200000 --range=$NUM --num=$NUM --reads=1000000 --bloom_bits=$BLOOMBITS                             --stats_interval=1000000 --value_size=100  --benchmarks=r100,r75,r50,r25,r0,stats           --ycsb_ops_num=$YCSB_OPS  --use_existing_db=1  --histogram=1       --log=false  --max_file_size=67108864 --write_buffer_size=67108864  --sleep=$SLEEP --print_wa=true | tee level_rw_ratio.log

