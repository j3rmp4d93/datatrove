cd datatrove/src/
conda activate /home/u231360/miniconda3/envs/datatrove
PYTHON="/home/u231360/miniconda3/envs/datatrove/bin/python3.10"
SCRIPT="fineweb-TC.py"
"$PYTHON" "$SCRIPT" "$1" "$2"
#until "$PYTHON" "$SCRIPT" "$1"; do
    #sleep 1800  # Optional: wait for a second before retrying
#done

#/home/u231360/miniconda3/envs/datatrove/bin/python3.10 fineweb-TC.py $1