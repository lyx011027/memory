
#!/bin/bash

if [ ! $# == 2 ]; then

echo "Usage: $0 DataPath BatchNumber"

exit

fi

filename="$1"

batchnum="$2"


if [ -e $filename ]
then
	if [[ $batchnum =~ ^[0-9]+$ ]]
    then
        python3 split.py "$filename" "$batchnum"
        python3 sample_multipro.py
        python3 rf_multipro.py
    else
        echo "BatchNumber is not number"
    fi
else
	echo " DataPath not found."
fi
