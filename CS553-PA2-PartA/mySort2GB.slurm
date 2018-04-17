#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=mySort2GB.log
#SBATCH --wait-all-nodes=1
split -n 16 -d /input/data-2GB.in /tmp/tmpFile 
/usr/bin/time -f "Time taken to sort 2GB in (seconds): \t%e" java -Xmx8182m MySort 2GB 4 5000000
valsort /tmp/FinalSortedFile	

