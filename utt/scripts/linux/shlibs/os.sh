OS_FLAVOR="Unknown"
NUM_CPUS=

if [ "$(uname -s)" = "Darwin" ]; then
    OS="OSX"
    if sw_vers -productVersion | grep "^10\.15" >/dev/null; then
        OS_FLAVOR="Catalina"
    fi
    NUM_CPUS=`sysctl -n hw.ncpu`
elif [ "$(uname -s)" = "Linux" ]; then
    OS="Linux"
    NUM_CPUS=`grep -c ^processor /proc/cpuinfo`
    if [ -f /etc/issue ]; then
        if grep Fedora /etc/issue >/dev/null; then
    	    OS_FLAVOR="Fedora"
        elif grep Ubuntu /etc/issue >/dev/null; then
            OS_FLAVOR="Ubuntu"
        fi
    fi
fi

#echo "OS: $OS"
#echo "OS Flavor: $OS_FLAVOR"
