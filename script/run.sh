# #!/bin/bash
# #ops
# read=(100 50 95 0 0)
# insert=(0 0 0 100 5)
# update=(0 50 5 0 0)
# delete=(0 0 0 0 0)
# range=(0 0 0 0 95)

# #fixed-op
# readonly=(100 0 0 0 0)
# updateonly=(0 0 100 0 0)

# #exp
# threads=(0 2 18 36 72 108 144)
# #threads=(0 2 16 32 64 96 128)
# mem_threads=(1 4)
# cache=(0 64 128 256 512 1024)
# distribution=(0 1 2 3)
# zipf=(0.99)
# bulk=200
# warmup=10
# runnum=200
# nodenum=5

# #other
# correct=0
# timebase=1
# early=1
# #index=(0 1 2)
# rpc=1
# admit=0.1
# tune=0

# SELF_IP=$(hostname -I | awk '{print $1}')

# for dis in 0
# do 
#     for op in 0
#     do 
#         for idx in 0
#         do  
#             for t in 1
#             do
#                 # ./restartMemc.sh
#                 sudo ../build/newbench $nodenum ${read[$op]} ${insert[$op]} ${update[$op]} ${delete[$op]} ${range[$op]} ${threads[$t]} ${mem_threads[0]} ${cache[3]} ${distribution[$dis]} ${zipf[0]} $bulk $warmup $runnum $correct $timebase $early $idx $rpc $admit $tune 36
#                 sleep 2
#             done
#         done
#     done
# done

#!/bin/bash
# ops
read=(100 50 95 0 0)
insert=(0 0 0 100 5)
update=(0 50 5 0 0)
delete=(0 0 0 0 0)
range=(0 0 0 0 95)

# fixed-op
readonly=(100 0 0 0 0)
updateonly=(0 0 100 0 0)

# exp
threads=(0 2 18 36 72 108 144)
# threads=(0 2 16 32 64 96 128)
mem_threads=(1 4)
cache=(0 64 128 256 512 1024)
distribution=(0 1 2 3)
zipf=(0.99)
bulk=50
warmup=10
runnum=50
nodenum=5

# other
correct=0
timebase=1
early=1
# index=(0 1 2)
rpc=1
admit=0.1
tune=0

SELF_IP=$(hostname -I | awk '{print $1}')

# Take four parameters for dis, op, idx, and t
dis=$1
op=$2
idx=$3
t=$4

# Validate input arguments (optional)
if [ -z "$dis" ] || [ -z "$op" ] || [ -z "$idx" ] || [ -z "$t" ]; then
    echo "Usage: $0 <dis> <op> <idx> <t>"
    exit 1
fi

# Execute the command with provided parameters
sudo ../build/xmdbench $nodenum ${read[$op]} ${insert[$op]} ${update[$op]} ${delete[$op]} ${range[$op]} ${threads[$t]} ${mem_threads[1]} ${cache[3]} ${distribution[$dis]} ${zipf[0]} $bulk $warmup $runnum $correct $timebase $early $idx $rpc $admit $tune 36

sleep 2