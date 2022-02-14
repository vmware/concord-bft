#!/usr/bin/env bash

# State Transfer SDDC controller - This tool helps a developer to perform command-line essential
# operations when analyzing or testing State Transfer.

# This script assumes that the output path is empty. If not, it fails with a warning.
set -eo pipefail

# For non self-explanatory commands, a comment above the command will explain how to use the command in more details
usage() {
    short_dash_line="---------------\n"
    printf "\n${short_dash_line}st-ctl usage:\n${short_dash_line}"
    printf "%s\n" " -h --help, print this message"
    printf "%s\n" " -s --set-concord-log-level <log level,string:TRACE|DEBUG|INFO|WARN|ERROR|FATAL>"
    printf "%s\n" " -c --show-concord-log-properties"
    printf "%s\n" " -m --comm-ctl <ip list,comma seperated ip list> <operation,string:down|up>"
    printf "%s\n\t%s\n" " -f --copy-from-multi <remotes source path,string> <local destination path,string> <remotes ip list,comma-seperated ip list>" \
           "<user name,string,optional,default:root> <password,string,optional,default:Bl0ckch@!n>"
    printf "%s\n\t%s\n" " -t --copy-to-multi <local source path,string> <remote destination path,string>" \
           "<remotes ip list,comma-seperated ip list> <user name,string,optional,default:root> <password,string,default:Bl0ckch@!n>"
    # install packages and create profile/bashrc files to enhance the working enviorment
    printf "%s\n" " -i --install-tools"
    printf "%s\n" " -g --gen-concord-coredump-summary <output_path,string> <container_name,string>"
    # If line number is given, version will be changed only for this line number
    printf "%s\n" " -a --agent-replace-version <current version,format:X.X.X.X.X> <new version,format:X.X.X.X.X> <line number,integer,optional>"
    printf "%s\n" " -v --agent-show-containers-version"
    printf "%s\n" " -r --reset-containers <agent version,format:X.X.X.X.X>"
    printf "%s\n\t%s\n" " -p --compress-truncate-container-logs <container_name,string> <output_folder_path,string> <repeat_times,integer>" \
        "<wait_before_iteration,seconds,optional,default=0> <wait_after_iteration,seconds,optional,default=0>"
    printf "%s\n" " -u --truncate-container-logs <container_name,string>"
    printf "%s\n" " -k --kill-concord-process"
    printf "%s\n" " -sr --show-rvb_data-state <period (seconds),integer,0 to print once only"
    printf "%s\n" " -ss --save-snapshot <snapshot name>"
    printf "%s\n" " -lds --load-snapshot <snapshot name>"
    printf "%s\n" " -lss --list-snapshots"
    printf "%s\n" " -bst --set-concord-bft-config-parameter <parameter name, string> <parameter value>"
    printf "%s\n" " -bsh --show-concord-bft-config"
    printf "%s\n" " -mcrab --mc-reset-all-buckets"
}

parser() {
    cmd_set_concord_log_level=false
    cmd_show_concord_log_properties=false
    cmd_comm_ctl=false
    cmd_copy_from_multi=false
    cmd_copy_to_multi=false
    cmd_install_tools=false
    cmd_gen_concord_coredump_summary=false
    cmd_agent_replace_version=false
    cmd_agent_show_containers_version=false
    cmd_reset_containers=false
    cmd_compress_truncate_container_logs=false
    cmd_truncate_container_logs=false
    cmd_kill_concord=false
    cmd_show_rvb_data_state=false
    cmd_save_snapshot=false
    cmd_load_snapshot=false
    cmd_list_snapshots=false
    cmd_set_concord_bft_config_parameter=false
    cmd_show_concord_bft_config=false
    cmd_mc_reset_all_buckets=false

    while [ "$1" ]; do
        case $1 in
        -h | --help)
        usage
        exit
        ;;

        -s | --set-concord-log-level)
        cmd_set_concord_log_level=true
        if [[ $# -lt 2 ]]; then echo "error: bad input for option -s | --set_concord_log_level!" >&2; usage; exit; fi
        concord_log_level=$2
        if [[ ! $concord_log_level = "TRACE" ]] && [[ ! $concord_log_level = "DEBUG" ]] && [[ ! $concord_log_level = "WARN" ]] && \
            [[ ! $concord_log_level = "ERROR" ]] && [[ ! $concord_log_level = "FATAL" ]] && [[ ! $concord_log_level = "INFO" ]]; then
            echo "error: bad log level $concord_log_level for option -s | --set_concord_log_level!" >&2
            usage
            exit
        fi
        break
        ;;

        -c | --show-concord-log-properties)
        cmd_show_concord_log_properties=true
        break
        ;;

        -m | --comm-ctl)
        cmd_comm_ctl=true
        if [[ $# -lt 3 ]] ; then echo "error: bad input for option -m | --comm_ctl!" >&2; usage; exit; fi
        ip_list=$2      # won't check for validity, too complicated
        operation=$3
        if [[ ! $operation = "down" ]] && [[ ! $operation = "up" ]]; then
            echo "error: bad log level $operation for option -m | --comm_ctl!" >&2
            usage
            exit
        fi
        break
        ;;

        -f | --copy-from-multi)
        cmd_copy_from_multi=true
        if [[ $# -lt 4 ]]; then echo "error: bad input for option -f | --copy_from_multi!" >&2; usage; exit; fi
        from_path=$2
        to_path=$3
        remotes_ip_list=$4
        if [[ $# -eq 4 ]] || [[ $5 == -* ]] || [ "$5" == "--*" ]; then
            user_name="root"
            password="Bl0ckch@!n"
        else
            user_name=$5
            password=$6
        fi
        break
        ;;

        -t | --copy-to-multi)
        cmd_copy_to_multi=true
        if [[ $# -lt 4 ]]; then echo "error: bad input for option -t | --copy_to_multi!" >&2; usage; exit; fi
        from_path=$2
        to_path=$3
        remotes_ip_list=$4
        if [[ $# -eq 4 ]] || [[ $5 == -* ]] || [ "$5" == "--*" ]; then
            user_name="root"
            password="Bl0ckch@!n"
        else
            user_name=$5
            password=$6
        fi
        break
        ;;

        -i | --install-tools)
        cmd_install_tools=true
        break
        ;;

        -g | --gen-concord-coredump-summary)
        cmd_gen_concord_coredump_summary=true
        if [[ $# -lt 3 ]]; then echo "error: bad input for option -g | --gen_concord_coredump_summary!" >&2; usage; exit; fi
        output_path=$2
        container_name=$3
        break
        ;;

        -a | --agent-replace-version)
        cmd_agent_replace_version=true
        if [[ $# -lt 3 ]]; then echo "error: bad input for option -a | --agent_replace_version!" >&2; usage; exit; fi
        cur_ver=$2
        new_ver=$3
        line=
        if [[ $# -eq 3 ]] || [[ $4 != -* ]] || [ "$4" != "--*" ]; then
            line=$4
        fi
        break
        ;;

        -v | --agent-show-containers-version)
        cmd_agent_show_containers_version=true
        break
        ;;

        -r | --reset-containers)
        cmd_reset_containers=true
        if [[ $# -lt 2 ]]; then echo "error: bad input for option -r | --reset_containers!" >&2; usage; exit; fi
        agent_version=$2
        break
        ;;

        -p | --compress-truncate-container-logs)
        cmd_compress_truncate_container_logs=true
        if [[ $# -lt 4 ]]; then echo "error: bad input for option -p | --compress_truncate_container_logs!" >&2; usage; exit; fi
        container_name=$2
        output_folder_path=$(realpath "$3")
        repeat_times=$4
        wait_before_iteration=0
        wait_after_iteration=0
        if [[ $# -eq 6 ]] || [[ $5 == -* ]] || [ "$5" == "--*" ]; then
            wait_before_iteration=$5
            wait_after_iteration=$6
        fi
        if [[ $# -eq 5 ]] || [[ $5 == -* ]] || [ "$5" == "--*" ]; then
            wait_before_iteration=$5
        fi
        if [ -d "${output_folder_path}" ]; then echo "${output_folder_path} already exist!"; exit 1; fi
        break
        ;;

        -u | --truncate-container-logs)
        cmd_truncate_container_logs=true
        if [[ $# -lt 2 ]]; then echo "error: bad input for option -u | --truncate-container-logs!" >&2; usage; exit; fi
        container_name=$2
        break
        ;;

        -k | --kill-concord-process)
        cmd_kill_concord=true
        break
        ;;

        -sr | --show-rvb_data-state)
        cmd_show_rvb_data_state=true
        if [[ $# -lt 2 ]]; then echo "error: bad input for option -sr | --show-rvb_data-state!" >&2; usage; exit; fi
        period_in_seconds=$2
        break
        ;;

        -ss | --save-snapshot)
        cmd_save_snapshot=true
        if [[ $# -lt 2 ]]; then echo "error: bad input for option -ss | --save-snapshot!" >&2; usage; exit; fi
        snapshot_name=$2
        break
        ;;

        -lds | --load-snapshot)
        cmd_load_snapshot=true
        if [[ $# -lt 2 ]]; then echo "error: bad input for option -ls | --load-snapshot!" >&2; usage; exit; fi
        snapshot_name=$2
        break
        ;;

        -lss | --list-snapshots)
        cmd_list_snapshots=true
        break
        ;;

        -bst | --set-concord-bft-config-parameter)
        cmd_set_concord_bft_config_parameter=true
        if [[ $# -lt 3 ]] ; then echo "error: bad input for option -bst | --set-concord-bft-config-parameter!" >&2; usage; exit; fi
        param_key=$2
        param_val=$3
        break
        ;;

        -bsh | --show-concord-bft-config-parameter)
        cmd_show_concord_bft_config=true
        break
        ;;

        -mcrab | --mc-reset-all-buckets)
        cmd_mc_reset_all_buckets=true
        break
        ;;

        *)
        echo "error: unknown input $1!" >&2
        usage
        exit
        ;;
        esac
    done
}

if [[ $# -eq 0 ]]; then
    usage
    exit 1
fi
parser "$@"

# Constants
concord_container_name="concord"
vm_agent_config_path="/config/agent/config.json"
concord_log_properties_path="/concord/resources/log4cplus.properties"
concord_bft_config_path="/concord/resources/bft_config.yaml"
minio_alias_name="min"
minio_command_line_tool_path="/mnt/data/mc"
####

##########################################
# handle cmd_set_concord_log_level
##########################################
if $cmd_set_concord_log_level; then
    declare -a arr=( \
        "concord.bft.st.dst" \
        "concord.bft.st.src" \
        "concord.util.handoff" \
        "concord.bft.st.dbdatastore" \
        "concord.bft.st.inmem" \
        "concord.bft.st.rvb" \
        "concord.storage.s3" \
        "concord.kvbc.S3KeyGenerator" \
        "skvbc.replica"

        # Uncomment as needed
        #"serializable" \
        #'rocksdb'
    )
    for logger in "${arr[@]}"
    do
      rc=$(docker exec -t ${concord_container_name} bash -c "grep -q \"$logger\" \"${concord_log_properties_path}\"; echo $?")
	    rc=`echo $rc | tr -d '\r'`
        if [[ "$rc" -eq 0 ]]; then
            docker exec ${concord_container_name} bash -c \
                "echo 'log4cplus.logger.$logger=${concord_log_level}' >> '${concord_log_properties_path}'"
        else
            docker exec ${concord_container_name} bash -c \
                "sed -i 's/.*${logger}.*/log4cplus.logger.$logger=${concord_log_level}/g' '${concord_log_properties_path}'"
        fi
    done
    docker exec ${concord_container_name} bash -c "cat '${concord_log_properties_path}'"
    echo "===Done!==="
fi

##########################################
# handle cmd_show_concord_log_properties
##########################################
if $cmd_show_concord_log_properties; then
    docker exec ${concord_container_name} bash -c "cat '${concord_log_properties_path}'"
fi

##########################################
# handle cmd_comm_ctl
##########################################
if $cmd_comm_ctl; then
    IFS=', ' read -r -a IPS <<< "$ip_list"
    for IP in "${IPS[@]}"; do
        if [[ "$operation" == "down" ]]; then
            CMD="-I"
            echo "blocking outgoing/incoming traffic, IP=$IP"
        else # up
            echo "Unblocking outgoing/incoming traffic, IP=$IP"
            CMD="-D"
        fi
        iptables $CMD DOCKER-USER -d "$IP" -j DROP
        iptables $CMD DOCKER-USER -s "$IP" -j DROP
    done
fi

##########################################
# handle cmd_copy_from_multi
##########################################
if $cmd_copy_from_multi; then
    IFS=', ' read -r -a remote_ips <<< "$remotes_ip_list"
    i=0
    for remote_ip in "${remote_ips[@]}"; do
        echo "copying from $remote_ip.."
        sshpass -p "${password}" \
            scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r \
            "${user_name}@${remote_ip}:${from_path}" "${to_path}" &
        pids[${i}]=$!
        ((i = i + 1))
    done

    # wait for all pids
    for pid in ${pids[*]}; do
        wait $pid
    done
    echo "===Done!==="
fi

##########################################
# handle cmd_copy_to_multi
##########################################
if $cmd_copy_to_multi; then
    IFS=', ' read -r -a remote_ips <<< "$remotes_ip_list"
    i=0
    for remote_ip in "${remote_ips[@]}"; do
        sshpass -p ${password} rsync  -avuq -e 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' \
            "${from_path}" "${user_name}@${remote_ip}:${to_path}" &
        pids[${i}]=$!
        ((i = i + 1))
    done

    # wait for all pids
    for pid in ${pids[*]}; do
        wait $pid
    done
    echo "===Done!==="
fi

##########################################
# handle cmd_install_tools
##########################################
if $cmd_install_tools; then
rm -rf ~/.tmux.conf ~/.profile ~/root/.mc/

cat <<EOF > ~/.tmux.conf
# Scroll History
set-option -g history-limit 10000000
# Set ability to capture on start and restore on exit window data when running an application
setw -g alternate-screen on

# Lower escape timing from 500ms to 50ms for quicker response to scroll-buffer access.
set -s escape-time 50

set-option -g mouse on
setw -g alternate-screen on
EOF

sed -i "s/^TMOUT=.*$/TMOUT=9000000/g" /etc/bash.bashrc
sed -i "s/^readonly TMOUT$/#readonly TMOUT/g" /etc/bash.bashrc
sed -i "s/^export TMOUT$/#export TMOUT/g" /etc/bash.bashrc

sed -i "s/^TMOUT=.*$/TMOUT=9000000/g" /etc/profile.d/tmout.sh
sed -i "s/^readonly TMOUT$/#readonly TMOUT/g" /etc/profile.d/tmout.sh
sed -i "s/^export TMOUT$/#export TMOUT/g" /etc/profile.d/tmout.sh

rpm -i https://packages.vmware.com/photon/3.0/photon_release_3.0_x86_64/x86_64/nano-3.0-1.ph3.x86_64.rpm || true
rpm -i https://packages.vmware.com/photon/3.0/photon_release_3.0_x86_64/x86_64/tmux-2.7-1.ph3.x86_64.rpm || true

cd /root/
rm -rf ./lnav-0.10.1 ./lnav-0.10.1-musl-64bit.zip
wget https://github.com/tstack/lnav/releases/download/v0.10.1/lnav-0.10.1-musl-64bit.zip
unzip lnav-0.10.1-musl-64bit.zip
mv lnav-0.10.1/lnav /usr/bin/
rm -rf ./lnav-0.10.1 ./lnav-0.10.1-musl-64bit.zip

cat <<EOF >> ~/.profile
alias ll="ls -la"
alias cd_grep_log_full="docker logs concord | grep -ia"
alias cd_grep_log_tail="docker logs concord --tail 10 -f | grep -ia"
alias cd_login="docker exec -it concord /bin/bash"
alias cd_logs_zip="docker logs concord | zip -9 log.zip -"

myip() {
  echo $(ifconfig | grep  -m 1 -Eo '10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}' | cut -d ":" -f 2 | cut -d " " -f 1 | grep -v 255)
}

_myid() {
  echo $(ls /config/concord/config-generated/ 2> /dev/null | cut -d "." -f 2)
}

my_id() {
  if [ "$(docker ps -a | grep concord | wc -l)" -eq "0" ]; then
    local id="ledger"
  else
    local id=\$(_myid)
    if [ -z "\$id" ]; then
      n=\$(grep -ai -A 1000000 ro_node /config/concord/config-local/deployment.config | grep -n \$(myip)  | cut -f1 -d:)
      re='^[0-9]+$'
      if [[ \$n =~ \$re ]] ; then
        n=\$((n-2))
        n=\$(grep -ai -A 1000000 ro_node /config/concord/config-local/deployment.config  | sed -n \${n}p | cut -f2 -d ':')
        id="id_ro_"\${n//[[:space:]]/}
      fi
    else
      id="id_"\$(_myid)
    fi
  fi
  echo "\${id}"
}

export PATH="$PATH:/root"

PS1='\[\e[0;31m\][\$(my_id || "")][ip_\$(myip)][\w]\n\[\e[m\]> '
export PS1
shopt -s checkwinsize
if [ ! -e "/logs" ] || [ ! -e "/mnt/data/logs/" ]; then
  mkdir -p /mnt/data/logs/
  rm /logs
  ln -s /mnt/data/logs/ /logs
fi

if [ ! -e "/coredump_logs" ] || [ ! -e "/mnt/data/coredump_logs/" ]; then
  mkdir -p /mnt/data/coredump_logs/
  rm /logs
  ln -s /mnt/data/coredump_logs/ /coredump_logs
fi

if [ ! -e "/keep" ] || [ ! -e "/mnt/data/keep/" ]; then
  mkdir -p /mnt/data/keep/
  rm /keep
  ln -s /mnt/data/keep/ /keep
fi
cd /mnt/data/

EOF

# inside concord container
docker exec -it ${concord_container_name} bash -c "apt update && apt install nano -y"  >/dev/null 2>&1 || true

. ~/.profile

# install minio command line tool 'mc' in RO replica
if [[ "$(my_id)" == *"id_ro"* ]]; then
  rm -rf ${minio_command_line_tool_path}*
  echo "Installing minio mc under /mnt/data/"
  wget https://dl.min.io/client/mc/release/linux-amd64/mc
  chmod +x ${minio_command_line_tool_path}
  ip=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-url | cut -d ":" -f 2)
  port=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-url | cut -d ":" -f 3)
  secret_key=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-secret-key | cut -d ' ' -f 6)
  access_key=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-access-key| cut -d ' ' -f 6)
  ip=$(echo ${ip} | xargs)
  port=$(echo ${port} | xargs)
  secret_key=$(echo ${secret_key} | xargs)
  access_key=$(echo ${access_key} | xargs)
  echo "Adding alias: ./mc alias set min http://${ip}:${port} ${access_key} ${secret_key}"
  ${minio_command_line_tool_path} alias set ${minio_alias_name}/ http://${ip}:${port} ${access_key} ${secret_key}
fi

echo "Done Installing tools, please re-log"
fi # if $cmd_install_tools; then

##########################################
# handle cmd_gen_concord_coredump_summary
##########################################
if $cmd_gen_concord_coredump_summary; then
    myip=$(ifconfig | grep  -m 1 -Eo '10\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}' | cut -d ":" -f 2 | cut -d " " -f 1 | grep -v 255)
    output_file="${output_path}/cores_summary_${myip}.log"
    rm -f "${output_file}" || true 2> /dev/null
    mkdir -p ${output_path}
    echo "Generating output file (this may take some time) ..."
    docker exec -it "${container_name}" bash -c \
        'for filename in /concord/cores/core.concord*; do echo "***bt for ${filename}:***"; echo "set pagination off" > ~/.gdbinit; gdb concord ${filename} -ex bt -ex quit; done' >> "${output_file}"
    echo "Done generating summary under ${output_file}"
fi

##########################################
# handle cmd_agent_replace_version
##########################################
if $cmd_agent_replace_version; then
    cur_ver_dot_count="${cur_ver//[^.]}"
    new_ver_dot_count="${new_ver//[^.]}"
    if [[ "${#cur_ver_dot_count}" -ne 4 ]]; then
      echo "cur_ver=${cur_ver} has wrong format. (format:X.X.X.X.X)"
      exit 1
    fi
    if [[ "${#new_ver_dot_count}" -ne 4 ]]; then 
      echo "cur_ver=${new_ver} has wrong format. (format:X.X.X.X.X)"
      exit 1
    fi

    echo "Before changing version:"
    grep -r "${cur_ver}" ${vm_agent_config_path}

    echo "Changing version:"
    set -x
    sed -i "${line}s/${cur_ver}/${new_ver}/" ${vm_agent_config_path}
    set +x

    echo "After changing version:"
    grep -r "${new_ver}" $vm_agent_config_path

    echo "Done changing version"
fi

##########################################
# handle cmd_agent_show_containers_version
##########################################
if $cmd_agent_show_containers_version; then
    echo "Agent configured versions:"
    grep -rn "vmwblockchain" ${vm_agent_config_path}
fi

##########################################
# handle cmd_reset_containers
##########################################
if $cmd_reset_containers; then
    agent_ver_dot_count="${agent_version//[^.]}"
    if [[ "${#agent_ver_dot_count}" -ne 4 ]]; then 
      echo "agent_version=${agent_version} has wrong format. (format:X.X.X.X.X)"
      exit 1
    fi

    docker stop $(docker ps -a -q) || true
    docker rm -f $(docker ps -a -q) || true
    rm -rf /config/daml-index-db/*
    rm -rf /mnt/data/db/*
    rm -rf /config/concord/config-generated/*
    rm -rf /mnt/data/rocksdbdata/*
    docker volume prune -f || true
    docker run -d --name=agent --restart=always \
                        --network=blockchain-fabric \
                        -p 127.0.0.1:8546:8546 \
                        -v /config:/config -v /var/run/docker.sock:/var/run/docker.sock \
                        blockchain-docker-internal.artifactory.eng.vmware.com/vmwblockchain/agent:${agent_version}
    echo "===Done!==="
fi

##########################################
# handle cmd_compress_truncate_container_logs
##########################################
if $cmd_compress_truncate_container_logs; then
    mkdir -p "${output_folder_path}"
    for (( c=1; c <= repeat_times; c++ )); do
        if [[ ${wait_before_iteration} -gt 0 ]]; then echo "sleeping ${wait_after_iteration} seconds (before).."; sleep "${wait_before_iteration}"; fi
        raw_log_path="${output_folder_path}/${container_name}_${c}.log"
        docker logs "${container_name}" > "${raw_log_path}"
        truncate -s 0 $(docker inspect --format='{{.LogPath}}' ${container_name})
        cd "${output_folder_path}"
        echo "${container_name} log truncated!" && date
        output_zip_file_name="${output_folder_path}/log_n${c}_$(date +"%Y_%m_%d_%R:%S").zip"
        zip -9 "${output_zip_file_name}" "${raw_log_path}"
        rm -rf "${raw_log_path}"
        if [[ ${wait_after_iteration} -gt 0 ]]; then echo "sleeping ${wait_after_iteration} seconds (after).."; sleep "${wait_after_iteration}"; fi
    done
    echo "===Done!==="
fi

##########################################
# handle cmd_truncate_container_logs
##########################################
if $cmd_truncate_container_logs; then
    truncate -s 0 "$(docker inspect --format='{{.LogPath}}' ${container_name})"
    echo "===Done!==="
fi

##########################################
# handle cmd_kill_concord
##########################################
if $cmd_kill_concord; then
    killall concord
    echo "===Done!==="
fi

##########################################
# handle cmd_show_rvb_data_state
##########################################
if $cmd_show_rvb_data_state; then
    for i in {1..1000000}; do
      docker exec -it concord bash -c "./concord-ctl status get state-transfer" | grep -oE "rvb_data_state.{0,70}"
      printf "\r"
      sleep ${period_in_seconds}
      if [[ ${period_in_seconds} -eq 0 ]]; then
        break;
      fi
    done
    echo "===Done!==="
fi

##########################################
# handle cmd_save_snapshot
##########################################
if $cmd_save_snapshot; then
  concord_exist=false
  daml_index_db_exist=false
  daml_ledger_api_exist=false
  concord_run=false
  daml_index_db_run=false
  daml_ledger_api_run=false

  snapshot_path="/mnt/data/snapshots/${snapshot_name}/"

  if [ -d ${snapshot_path} ]; then
    echo "Error: path ${snapshot_path} already exist!"
    exit 1
  fi

  mkdir -p ${snapshot_path}
  echo "Created folder ${snapshot_path}..."

  if [ $(docker ps -a | grep concord | wc -l) -eq '1' ]; then
    concord_exist=true
    if [[ ! "$(docker top concord 2>&1 | grep 'is not run' | wc -l)" -eq "1" ]]; then
      concord_run=true;
      echo "Stop: docker stop concord"
      docker stop concord
    fi
    echo "Copy: cp -R -a /mnt/data/rocksdbdata ${snapshot_path}"
    cp -R -a /mnt/data/rocksdbdata ${snapshot_path}
    echo "Copy: cp -R -a /config/concord/config-generated ${snapshot_path}"
    cp -R -a /config/concord/config-generated ${snapshot_path}
  fi

  if [ $(docker ps -a | grep daml_index_db | wc -l) -eq '1' ]; then
    daml_index_db_exist=true
    if [[ ! "$(docker top daml_index_db 2>&1 | grep 'is not run' | wc -l)" -eq "1" ]]; then
      daml_index_db_run=true;
      echo "Stop: docker stop daml_index_db"
      docker stop daml_index_db
    fi
    echo "Copy: cp -R -a /mnt/data/db ${snapshot_path}"
    cp -R -a /mnt/data/db ${snapshot_path}
    echo "Copy: cp -R -a /config/daml-index-db ${snapshot_path}"
    cp -R -a /config/daml-index-db ${snapshot_path}
  fi

  if [ $(docker ps -a | grep daml_ledger_api | wc -l) -eq '1' ]; then
    daml_ledger_api_exist=true
    if [[ ! "$(docker top daml_ledger_api 2>&1 | grep 'is not run' | wc -l)" -eq "1" ]]; then 
      daml_ledger_api_run=true;
      echo "Stop: docker stop daml_ledger_api"
      docker stop daml_ledger_api
    fi
  fi

  if $daml_ledger_api_run; then
    echo "Restart: docker restart daml_ledger_api"
    docker restart daml_ledger_api
  fi
  if $daml_index_db_run; then
    echo "Restart: docker restart daml_index_db"
    docker restart daml_index_db
  fi
  if $concord_run; then
    echo "Restart: docker restart concord"
    docker restart concord
  fi
  echo "-------------------"
  echo "Done! snapshot is under ${snapshot_path}"
  echo "Don't forget to run script under particpants if you would like having background load during ST."
  echo "(RO replica is not supported!)"
  echo "-------------------"
fi

##########################################
# handle cmd_load_snapshot
##########################################
if $cmd_load_snapshot; then
  concord_exist=false
  daml_index_db_exist=false
  daml_ledger_api_exist=false
  concord_run=false
  daml_index_db_run=false
  daml_ledger_api_run=false

  snapshot_path="/mnt/data/snapshots/${snapshot_name}/"

  if [ ! -d ${snapshot_path} ]; then
    echo "Error: path ${snapshot_path} does not exist!"
    exit 1
  fi

  echo "Found snapshot under ${snapshot_path}, are you sure you want to replace it with current one (y/n)?"
  read answer
  if [[ ! "${answer}" == "y" ]]; then
    exit 0
  fi

  mkdir -p ${snapshot_path}
  echo "Created folder ${snapshot_path}..."

  if [ $(docker ps -a | grep concord | wc -l) -eq '1' ]; then
    concord_exist=true
    if [[ ! "$(docker top concord 2>&1 | grep 'is not run' | wc -l)" -eq "1" ]]; then
      concord_run=true;
      echo "Stop: docker stop concord"
      docker stop concord
    fi
    echo "Delete: rm -rf /mnt/data/rocksdbdata/*"
    rm -rf /mnt/data/rocksdbdata/*
    echo "Copy: cp -R -a ${snapshot_path}/rocksdbdata/* /mnt/data/rocksdbdata/"
    cp -R -a ${snapshot_path}/rocksdbdata/* /mnt/data/rocksdbdata/
    echo "Delete: rm -rf /config/concord/config-generated/*"
    rm -rf /config/concord/config-generated/*
    echo "Copy: cp -R -a ${snapshot_path}/config-generated/* /config/concord/config-generated/"
    cp -R -a ${snapshot_path}/config-generated/* /config/concord/config-generated/
  fi

  if [ $(docker ps -a | grep daml_index_db | wc -l) -eq '1' ]; then
    daml_index_db_exist=true
    if [[ ! "$(docker top daml_index_db 2>&1 | grep 'is not run' | wc -l)" -eq "1" ]]; then
      daml_index_db_run=true;
      echo "Stop: docker stop daml_index_db"
      docker stop daml_index_db
    fi
    echo "Delete: rm -rf /mnt/data/db/*"
    rm -rf /mnt/data/db/*
    echo "Copy: cp -R -a ${snapshot_path}/db/* /mnt/data/db/"
    cp -R -a ${snapshot_path}/db/* /mnt/data/db/
    echo "Delete: rm -rf /config/daml-index-db/*"
    rm -rf /config/daml-index-db/*
    echo "Copy: cp -R -a ${snapshot_path}/daml-index-db/* /config/daml-index-db/"
    cp -R -a ${snapshot_path}/daml-index-db/* /config/daml-index-db/ || true
  fi

  if [ $(docker ps -a | grep daml_ledger_api | wc -l) -eq '1' ]; then
    daml_ledger_api_exist=true
    if [[ ! "$(docker top daml_ledger_api 2>&1 | grep 'is not run' | wc -l)" -eq "1" ]]; then 
      daml_ledger_api_run=true;
      echo "Stop: docker stop daml_ledger_api"
      docker stop daml_ledger_api
    fi
  fi

  if $daml_ledger_api_run; then
    echo "Restart: docker restart daml_ledger_api"
    docker restart daml_ledger_api
  fi
  if $daml_index_db_run; then
    echo "Restart: docker restart daml_index_db"
    docker restart daml_index_db
  fi
  if $concord_run; then
    echo "Restart: docker restart concord"
    docker restart concord
  fi
  echo "-------------------"
  echo "Done! snapshot is under ${snapshot_path}"
  echo "Don't forget to run script under particpants if you would like having background load during ST."
  echo "(RO replica is not supported!)"
  echo "-------------------"
fi

##########################################
# handle cmd_list_snapshots
##########################################
if $cmd_list_snapshots; then
  echo "Total $(cd /mnt/data/snapshots; ls -d * | wc -l) snapshots:"
  cd /mnt/data/snapshots/
  for i in $(ls -d *); do
    echo ${i};
  done
fi

##########################################
# handle cmd_set_concord_bft_config_parameter
##########################################
if $cmd_set_concord_bft_config_parameter; then
  count=$(docker exec ${concord_container_name} bash -c "grep ${param_key}: ${concord_bft_config_path} | wc -l")
  if [[ ${count} -ne 1 ]]; then
    echo "Error: Parameter ${param_key} should be exactly once in ${concord_bft_config_path} inside container ${concord_container_name}!"
    exit 1
  fi
  docker exec ${concord_container_name} bash -c "sed -r -i 's/^(\s*${param_key}\s*:\s*).*/\1${param_val}/' '$concord_bft_config_path'"
  echo "===Done!==="
fi

##########################################
# handle cmd_show_concord_bft_config
##########################################
if $cmd_show_concord_bft_config; then
  docker exec ${concord_container_name} bash -c "cat '${concord_bft_config_path}'"
fi

##########################################
# handle cmd_mc_reset_all_buckets
##########################################
if $cmd_mc_reset_all_buckets; then
  ip=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-url | cut -d ":" -f 2)
  secret_key=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-secret-key | cut -d ' ' -f 6)
  access_key=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-access-key| cut -d ' ' -f 6)
  port=$(cat /config/concord/config-local/deployment.config | grep -m1 s3-url | cut -d ":" -f 3)
  ip=$(echo ${ip} | xargs)
  secret_key=$(echo ${secret_key} | xargs)
  access_key=$(echo ${access_key} | xargs)
  port=$(echo ${port} | xargs)

  # delete all buckets
  echo sshpass -p 'ca$hc0w' ssh -o StrictHostKeychecking=no  "root@${ip}" \
    "docker stop minio > /dev/null 2>&1; docker rm minio > /dev/null 2>&1;" \
    "docker run -d -t --rm  -p ${port}:${port} -e MINIO_ACCESS_KEY='${access_key}'" \
    " -e MINIO_SECRET_KEY='${secret_key}' -e MINIO_HTTP_TRACE=/dev/stdout" \
    " --name=minio athena-docker-local.artifactory.eng.vmware.com/minio/minio:RELEASE.2020-09-05T07-14-49Z server /data" \
    " sleep 1; docker ps -a"

  sshpass -p 'ca$hc0w' ssh -o StrictHostKeychecking=no  "root@${ip}" \
    "docker stop minio > /dev/null 2>&1; docker rm minio > /dev/null 2>&1;" \
    "docker run -d -t --rm  -p ${port}:${port} -e MINIO_ACCESS_KEY='${access_key}'" \
    " -e MINIO_SECRET_KEY='${secret_key}' -e MINIO_HTTP_TRACE=/dev/stdout" \
    " --name=minio athena-docker-local.artifactory.eng.vmware.com/minio/minio:RELEASE.2020-09-05T07-14-49Z server /data;" \
    " sleep 1; docker ps -a"

  # create new buckets
  bucket_names=($(cat /config/concord/config-local/deployment.config | grep s3-bucket-name | cut -d ":" -f 2))
  for bn in "${bucket_names[@]}"
  do
    bn=$(echo ${bn} | xargs)
    echo bn=$bn
    echo "${minio_command_line_tool_path} mb ${minio_alias_name}/${bn}"
    ${minio_command_line_tool_path} mb ${minio_alias_name}/${bn}
  done
  ${minio_command_line_tool_path} ls ${minio_alias_name}
  echo "===Done!==="
fi

