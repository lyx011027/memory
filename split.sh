
#!/bin/bash

if [ ! $# == 1 ]; then

echo "Usage: $0 DataPath"
# /home/ms-admin/yixuan/data 1
exit

fi

filename="$1"

rm -rf split

python3 split_New.py "$filename"
# python3 sample_multipro.py
# python3 rf_multipro.py

