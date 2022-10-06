if [ -z "$UTT_HAS_ENV_SET" ]; then
    echo "ERROR: You must call 'source set-env.sh <build-type>' first"
    exit 1
fi

#which clang++ 2>&1 >/dev/null || { echo "ERROR: Clang is not installed."; exit 1; }
#which parallel 2>&1 >/dev/null || { echo "ERROR: GNU parallel needs to be installed."; exit 1; } 
