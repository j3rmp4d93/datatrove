qsub -l walltime=24:00:00,nodes=2:ram384gb:ppn=2 -d . job.sh -F "-dump=CC-MAIN-2023-23 -num_worker=8"
qsub -l walltime=24:00:00,nodes=2:ram384gb:ppn=2 -d . job.sh -F "-dump=CC-MAIN-2023-23 -num_worker=8" -W depend=afterok:XXXXJOBID