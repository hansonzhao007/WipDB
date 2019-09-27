mkdir ./kv/release
cd ./kv/release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j32 && cd ../../
mkdir ./pebblesdb/release
cd ./pebblesdb/release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j32 && cd ../../
mkdir ./leveldb/release
cd ./leveldb/release && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j32 && cd ../../
mkdir ./rocksdb/release
cd ./rocksdb/release && cmake -DCMAKE_BUILD_TYPE=Release  .. && make -j32 && cd ../../
