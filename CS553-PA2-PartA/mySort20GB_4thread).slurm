#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=mysort20GB.log
#SBATCH --wait-all-nodes=1
/usr/bin/time -f "Time taken to sort 20GB in (seconds): \t%e" java MySort 20GB 4 10000000
valsort /tmp/FinalSortedFile.txt

