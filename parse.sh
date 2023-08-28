
#!/bin/bash

if [ ! $# == 1 ]; then

echo "Usage: $0 DataPath"

exit

fi

filename="$1"

python3 parseLog.py "$filename" 

