echo fb0-=0-= | sudo -S bash -c 'echo 800000 > /proc/sys/fs/file-max'
ulimit -n 800000

# insert 50M keys. around 5G
rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
echo fb0-=0-= | sudo -S fstrim /mnt/ssd
echo fb0-=0-= | sudo -S fstrim /mnt/nvm
echo fb0-=0-= | sudo -S bash -c 'echo 1 > /proc/sys/vm/drop_caches'

for i in {50..2000..50};
do
NUM=$(( 50000*i ))
echo "=========== $NUM keys. $i partition ============" 
rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv_logload  --logpath=/mnt/ssd --use_existing_db=false --log=true --partition=$i  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandom,stats  --bloom_bits=10         --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true    --print_wa=true 
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv_logload  --logpath=/mnt/ssd --use_existing_db=true  --log=true --partition=$i  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillrandom,stats  --bloom_bits=10         --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true    --print_wa=true | tee kvlogload.log
grep "Log_Recover_Time" kvlogload.log  >> log_recover_uniform.txt
done


for i in {50..2000..50};
do
NUM=$(( 50000*i ))
echo "=========== $NUM keys. $i partition ============" 
rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv_logload  --logpath=/mnt/ssd --use_existing_db=false --log=true --partition=$i  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillexp,stats  --bloom_bits=10         --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true    --print_wa=true 
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv_logload  --logpath=/mnt/ssd --use_existing_db=true  --log=true --partition=$i  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillexp,stats  --bloom_bits=10         --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true    --print_wa=true | tee kvlogload.log
grep "Log_Recover_Time" kvlogload.log  >> log_recover_exponential.txt
done


for i in {50..2000..50};
do
NUM=$(( 50000*i ))
echo "=========== $NUM keys. $i partition ============" 
rm -rf /mnt/nvm/*
rm /mnt/ssd/*.log
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv_logload  --logpath=/mnt/ssd --use_existing_db=false --log=true --partition=$i  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillzipfian,stats  --bloom_bits=10         --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true    --print_wa=true 
cgexec -g memory:kv64 ./kv/release/tests/db/kv_bench --db=/mnt/nvm/kv_logload  --logpath=/mnt/ssd --use_existing_db=true  --log=true --partition=$i  --num=$NUM --value_size=100 --batch_size=1000 --benchmarks=fillzipfian,stats  --bloom_bits=10         --cache_size=8388608 --low_pool=3 --high_pool=3 --open_files=40000 --stats_interval=100000000 --histogram=true --compression=0     --write_buffer_size=2097152 --skiplistrep=false --log_dio=true    --print_wa=true | tee kvlogload.log
grep "Log_Recover_Time" kvlogload.log  >> log_recover_zipfian.txt
done