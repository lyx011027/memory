
#!/bin/bash

if [ ! $# == 2 ]; then

echo "Usage: $0 DataPath BatchNumber"

exit

fi

filename="$1"

batchnum="$2"

python3 split.py "$filename" "$batchnum"
python3 sample_multipro.py
python3 rf_multipro.py

