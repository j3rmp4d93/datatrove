qsub -I -l nodes=1:ram192gb:ppn=2 -d . 
qsub -I -l walltime=00:30:00 -d .