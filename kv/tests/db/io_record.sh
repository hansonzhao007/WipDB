now=$(date '+%Y/%m/%d-%H:%M:%S')
echo ${now}


trap "echo ${now} >> iolog; echo q| htop -C | aha --line-fix | html2text -width 1400 | grep kv_bench >> iolog;" SIGUSR1

trap "exit;" SIGUSR2

while true; do
	: # Do nothing
done