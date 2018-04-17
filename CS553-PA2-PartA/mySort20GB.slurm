#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=mySort20GB.log
#SBATCH --wait-all-nodes=1
split -n 40 -d /input/data-20GB.in /tmp/tmpFile
/usr/bin/time -f "Time taken to sort 20GB in (seconds): \t%e" java -Xmx8192m MySort 20GB 4 20000000 
valsort /tmp/FinalSortedFile

