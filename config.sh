# hugepage
echo 16384 > /proc/sys/vm/nr_hugepages


# cgroup
mkdir /sys/fs/cgroup/memory/kv4
echo 4G > /sys/fs/cgroup/memory/kv4/memory.limit_in_bytes
echo 0 > /sys/fs/cgroup/memory/kv4/memory.swappiness

mkdir /sys/fs/cgroup/memory/kv16
echo 16G > /sys/fs/cgroup/memory/kv16/memory.limit_in_bytes
echo 0 > /sys/fs/cgroup/memory/kv16/memory.swappiness

mkdir /sys/fs/cgroup/memory/kv64
echo 64G > /sys/fs/cgroup/memory/kv64/memory.limit_in_bytes
echo 0 > /sys/fs/cgroup/memory/kv64/memory.swappiness

mkdir /sys/fs/cgroup/memory/kv80
echo 77G > /sys/fs/cgroup/memory/kv80/memory.limit_in_bytes
echo 0 > /sys/fs/cgroup/memory/kv80/memory.swappiness

chown -R hanson:hanson /sys/fs/cgroup/memory/kv*


# mount 
mount /dev/sdb /mnt/ssd # where the store write log
mount /dev/nvme0n1 /mnt/nvm # where the store keep all the records

# turn off the journaling
tune2fs -O ^has_journal /dev/nvme0n1
tune2fs -O ^has_journal /dev/sdb

# set CPU in performance mode
cmd='-g performance'
MAX_CPU=$((`nproc --all` - 1))
for i in $(seq 0 $MAX_CPU); do
    echo "Changing CPU " $i " with parameter "$cmd;
    cpufreq-set -c $i $cmd ;
done