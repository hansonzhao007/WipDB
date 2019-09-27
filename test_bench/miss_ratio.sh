
evens=cache-references,cache-misses,dTLB-loads,dTLB-load-misses


echo "bucket, insert_speed(Kops/s), cache_ref, cache_miss, tlb_load, tlb_miss" > miss_ratio_hash.log
for i in {50..2000..50};
do
NUM=$(( 10000*i ))
echo "=========== HashTable Test. $NUM keys. $i partition ============" 
var_bucket_speed=`perf stat -o res.log -e $evens ./kv/release/tests/table/test_miss_ratio_hash_test --partition=$i --num=$NUM | grep Hashtable_Random_Insertion_Speed | awk '{gsub(/\,/,"")}1' | awk '{print $2, ", ", $4 }'` 
var_cache_ref=`grep cache-references res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1 }'`
var_cache_miss=`grep cache-miss res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_load=`grep dTLB-loads res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_miss=`grep dTLB-load-misses res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_res="$var_bucket_speed, $var_cache_ref, $var_cache_miss, $var_tlb_load, $var_tlb_miss"
echo $var_res >> miss_ratio_hash.log
done


echo "bucket, insert_speed(Kops/s), cache_ref, cache_miss, tlb_load, tlb_miss" > miss_ratio_hash_hugepage.log
for i in {50..2000..50};
do
NUM=$(( 10000*i ))
echo "=========== HashTableHugePage Test. $NUM keys. $i partition ============" 
var_bucket_speed=`perf stat -o res.log -e $evens ./kv/release/tests/table/test_miss_ratio_hash_hugepage_test --partition=$i --num=$NUM | grep Hashtable_Random_Insertion_Speed | awk '{gsub(/\,/,"")}1' | awk '{print $2, ", ", $4 }'` 
var_cache_ref=`grep cache-references res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1 }'`
var_cache_miss=`grep cache-miss res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_load=`grep dTLB-loads res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_miss=`grep dTLB-load-misses res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_res="$var_bucket_speed, $var_cache_ref, $var_cache_miss, $var_tlb_load, $var_tlb_miss"
echo $var_res >> miss_ratio_hash_hugepage.log
done


echo "bucket, insert_speed(Kops/s), cache_ref, cache_miss, tlb_load, tlb_miss" > miss_ratio_skip.log
for i in {50..2000..50};
do
NUM=$(( 10000*i ))
echo "=========== SkipList Test. $NUM keys. $i partition ============" 
var_bucket_speed=`perf stat -o res.log -e $evens ./kv/release/tests/table/test_miss_ratio_skip_test --partition=$i --num=$NUM | grep Skiplist_Random_Insertion_Speed | awk '{gsub(/\,/,"")}1' | awk '{print $2, ", ", $4 }'` 
var_cache_ref=`grep cache-references res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1 }'`
var_cache_miss=`grep cache-miss res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_load=`grep dTLB-loads res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_miss=`grep dTLB-load-misses res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_res="$var_bucket_speed, $var_cache_ref, $var_cache_miss, $var_tlb_load, $var_tlb_miss"
echo $var_res >> miss_ratio_skip.log
done


echo "bucket, insert_speed(Kops/s), cache_ref, cache_miss, tlb_load, tlb_miss" > miss_ratio_one_skip.log
for i in {50..2000..50};
do
NUM=$(( 10000*i ))
echo "=========== One SkipList Test. $NUM keys. ============" 
var_bucket_speed=`perf stat -o res.log -e $evens ./kv/release/tests/table/test_miss_ratio_one_skip_test --partition=$i --num=$NUM | grep Skiplist_Random_Insertion_Speed | awk '{gsub(/\,/,"")}1' | awk '{print $2, ", ", $4 }'` 
var_cache_ref=`grep cache-references res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1 }'`
var_cache_miss=`grep cache-miss res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_load=`grep dTLB-loads res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_tlb_miss=`grep dTLB-load-misses res.log | awk '{gsub(/\,/,"")}1' | awk '{print   $1}'`
var_res="$var_bucket_speed, $var_cache_ref, $var_cache_miss, $var_tlb_load, $var_tlb_miss"
echo $var_res >> miss_ratio_one_skip.log
done