while true
do
    CURRENTDATE=`date +"%Y/%m/%d %T"`
    echo ${CURRENTDATE} >> ~/size.txt
    du -cksh /mnt/ssd/* | sort -rn | head >> ~/size.txt
    sleep 50
done