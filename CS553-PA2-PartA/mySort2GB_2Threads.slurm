#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=mysort2GB_2thread.log
#SBATCH --wait-all-nodes=1 
/usr/bin/time -f "Time taken to sort 2GB in (seconds): \t%e"java MySort 2GB 2 5000000
valsort /tmp/FinalSortedFile.txt	

