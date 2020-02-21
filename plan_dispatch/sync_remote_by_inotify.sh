#!/bin/bash

src=/home/ccduser/data/
dest=gwac@190.168.1.15:/data/F30_data_proc

expect sync_re_sh.sh $src $dest

#inotifywait -mr --timefmt '%Y/%d/%m %H:%M:%S' --format '%T %e %w %f' -e create,delete,modify $src | while read text
inotifywait -mr --timefmt '%Y/%d/%m %H:%M:%S' --format '%T %e %w %f' -e create,delete $src | while read text
do
    echo -e "\n\n$text\n"
    expect sync_re_sh.sh $src $dest
    date=`date`
    echo -e "\n$date\n\n"
done