print_usage() {
    echo "Usage: $0 <file1.csv> [<file2.csv> ...] <cols>"
    echo
    echo "You can use 'all' for <cols>."
    echo
    echo "Example: $0 some-file.csv other-file.csv 1,4,5"
    echo "Example: $0 some-file.csv all"
}

get_num_cols() {
    file1=$1
    num_cols=0;
    for f in `cat "$file1" | head -n 1 | tr ',' '\n'`; do 
        num_cols=$((num_cols+1))
    done
    echo $num_cols
}

get_cols() {
    file1=$1
    echo
    num_cols=0;
    for f in `cat "$file1" | head -n 1 | tr ',' '\n'`; do 
        num_cols=$((num_cols+1))
        echo $num_cols - $f
    done
}

assert_same_cols() {
    prev_file=$1
    prev_cols=`get_cols $prev_file`
    shift

    while [ $# -gt 0 ]; do
        cols=`get_cols $1`
        
        if [ "$prev_cols" != "$cols" ]; then
            echo "ERROR: '$file1' and '$1' have different columns!" 
            echo
            echo "$prev_file:"
            echo "$prev_cols"
            echo
            echo "$1:"
            echo "$cols"
            
            exit 1
        fi
        
        shift
    done
}

hscroll_size=20

file1=$1
shift
files=$file1

while [ -f $1 ] && [ $# -gt 0 ]; do
    files="$files $1"
    shift
done

# INVARIANT: every file in $files exists and we might have extra argument(s) that are NOT files

# if we parsed all files from command args and no columns were specified as args, then print usage, then print the columns, then exit (see next chunks)
if [ $# -eq 0 ]; then
    print_usage
    if [ -z "$files" ]; then
        exit
    fi
fi

if [ $# -gt 1 ]; then
    shift
    echo "ERROR: You gave $(($#-1)) extra argument(s): $@"
    echo
    print_usage
    exit 1
fi

assert_same_cols $files

arg_cols=$1
num_cols=`get_num_cols $file1`

# print fields of CSV file, even if args are bad
echo
echo "Columns in files $files:"
echo "$(get_cols $file1)"

# exit, if no columns were specified
if [ $# -lt 1 ]; then
    exit 1 
fi

# if 'all' columns were specified then build comma separated list of all columns
if [ "$arg_cols" == "all" ]; then
    sel_cols="1"
    for c in `seq 2 $num_cols`; do
        sel_cols="$sel_cols,$c"
    done 
else
    sel_cols=$arg_cols
fi
first_col=`echo $sel_cols | cut -f1 -d,`

echo "Columns:        $sel_cols"
echo "Sort by column: $first_col"
echo "Files:          $files"

( head -n 1 $file1; for f in $files; do tail -n +2 $f; done | sort -g -t',' -k$first_col ) | cut -d',' -f$sel_cols | sed -e 's/,,/, ,/g' | column -s, -t | less -#$hscroll_size -N -S
