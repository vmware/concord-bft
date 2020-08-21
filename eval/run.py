import sys
import os, errno
import subprocess
import base64
from paramiko import SSHClient
from paramiko import AutoAddPolicy
import datetime
import pwd
import json
import sys
from optparse import OptionParser
from time import sleep
import threading

print_cmd_output = 1

def get_username():
    return pwd.getpwuid(os.getuid())[ 0 ]

def get_homedir():
    #return '/home/' + get_username() + '/'
    return '/root/'

def get_expdir():
    return "build/bftengine/tests/simpleTest/"

def setup_sshclient(host):
    client = SSHClient()
    client.set_missing_host_key_policy(AutoAddPolicy())
    client.load_system_host_keys()
    print "connection to: " + host
    client.connect(host, username=get_username())
    return client

def close_sshclient(client):
    client.close()

def exec_local_cmd(cmd):
    print("Running local command %s" % (cmd))
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    global print_cmd_output
    if print_cmd_output:
        for line in p.stdout.readlines():
            print line.rstrip('\n')
    retval = p.wait()
    return retval

def exec_remote_cmd(client, cmd):
    print("Running remote command %s" % (cmd))
    _, stdout, stderr = client.exec_command(cmd)
    all_lines = stdout.readlines() + stderr.readlines()
    global print_cmd_output
    if print_cmd_output:
        for line in all_lines:
            print line.rstrip('\n')
    return all_lines

def create_dir(dir_name):
    try:
        os.makedirs(dir_name)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def is_remote_process_running(client, process_name):
    cmd = "ps -ef | grep %s | grep -v grep | wc -l" % process_name
    log = exec_remote_cmd(client, cmd)
    log = int(log[0])
    #print log
    if (log == 0):
        return False
    else:
        return True

def setup_local_logs():
    # create a local directory to store logs
    create_dir("logs")

    # create directory for experiment results
    now = datetime.datetime.now()

    # create a directory to store log files
    dir_name = "logs/%d_%d_%d/%d_%d_%s" % (now.year, now.month, now.day, now.hour, now.minute, now.second)
    create_dir(dir_name)
    print("Log directory is %s" % dir_name)

    return dir_name

def setup_experiment_config(servers, clients, num_replicas, per_server_threads, per_client_threads):
    exec_local_cmd("./build.sh;")
    exec_local_cmd('''export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib && export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/usr/local/include;
../build/tools/GenerateConcordKeys -n %d -f %d -o private_replica_;''' % (num_replicas, (num_replicas-1)//3))
    
    fp = open("test_config.txt", "w")
    fp.write("replicas_config:\n")
    for server, num_replicas_per_server in zip(servers, per_server_threads):
        for i in range(num_replicas_per_server):
            port = 4400 + i * num_replicas
            fp.write("-" + server + ":" + str(port) + "\n")
    fp.write("clients_config:\n")
    for client, num_client_threads_per_client in zip(clients, per_client_threads):
        for i in range(num_client_threads_per_client):
            port = 3000 + i * num_replicas
            fp.write("-" + client + ":" + str(port) + "\n")

def setup_remote_env(client, host, do_install, network):
    # permission for keys
    exec_local_cmd("scp -p " + get_homedir() + "/.ssh/* " + host + ":" + get_homedir() + "/.ssh/")

    exec_remote_cmd(client, "chmod 600 %s/.ssh/id_rsa*" % (get_homedir()))
    exec_remote_cmd(client, "sudo usermod -s /bin/bash %s" % (get_username()))

    # clone the codebase
    repo_name = "concord-bft"
    git_home = "https://github.com/MaggieQi/concord-bft"

    if do_install:
        exec_remote_cmd(client, "sudo apt-get update; sudo apt-get install -y git")
        exec_remote_cmd(client, "cd %s/eval; ./install_concord_deps.sh" % (repo_name))

    exec_remote_cmd(client, "rm -rf %s" % (repo_name))
    print "Cloing code..."
    exec_remote_cmd(client, "git clone %s" % (git_home))

    # reset repo
    exec_remote_cmd(client, "cd %s; git reset --hard; git pull --rebase" % (repo_name))
    exec_remote_cmd(client, "cd %s; git checkout add_archipelago" % (repo_name))

    #build code
    if network == 'tcp':
        exec_remote_cmd(client, "cd %s; rm -rf build; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_COMM_TCP_PLAIN=TRUE ..; make -j 16;" % (repo_name))
    else:
        #exec_remote_cmd(client, "sudo sysctl -w net.core.rmem_max=41943040; sudo sysctl -w net.core.wmem_max=41943040; sudo sysctl -w net.core.netdev_max_backlog=4000;")
        exec_remote_cmd(client, "cd %s; rm -rf build; mkdir build; cd build; cmake -DCMAKE_BUILD_TYPE=Release ..; make -j 16;" % (repo_name))
    
    exec_local_cmd("scp private_replica* " + host + ":" + get_homedir() + '/' + repo_name + '/' + get_expdir())
    exec_local_cmd("scp test_config.txt " + host + ":" + get_homedir() + '/' + repo_name + '/' + get_expdir())

def teardown_remote_env(client):
    server_kill_cmd = "pkill -9 server; pkill -9 client;"
    exec_remote_cmd(client, "%s" % (server_kill_cmd))

    # remove old logs
    cmd = "rm -f /dev/shm/*.log"
    exec_remote_cmd(client, cmd)
    
def run_experiment_server(hostid, client, config_object):
    print "Running experiment"

    base_dir = "concord-bft"
    replica_id = hostid
    dynamicCollector = 1
    maxBatchSize = 1
    commitDuration = 0

    #-vc -vct <viewChangeTimeout> -stopseconds <stopSeconds>
    cmd = '''cd %s;
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib && export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/usr/local/include;
cd %s;
rm core;
ulimit -n 4096;ulimit -c unlimited;
./server -id %d -c %d -r %d -cf %s -a %s -dc %d -mb %d -commit %d  &> /dev/shm/replica%d.log &''' %(base_dir, get_expdir(), replica_id, config_object["num_client_threads"], config_object["num_replicas"], "test_config.txt", 
    config_object["system"], dynamicCollector, maxBatchSize, commitDuration, replica_id)
    print(cmd)
    exec_remote_cmd(client, cmd)

def run_experiment_client(hostid, client, config_object):
    print "Running experiment on client"

    base_dir = "concord-bft"
    client_id = hostid
    numberOperations = 2800

    cmd = '''cd %s;
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib && export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/usr/local/include;
cd %s;
rm core;
ulimit -c unlimited;
./client -id %d -cl %d -r %d -cf %s -a %s -f %d -i %d -srft 2 -srpt 2 -minrt 50 -maxrt 2000 -irt 150 &> /dev/shm/client%d.log;''' %(base_dir, get_expdir(), client_id, config_object["num_client_threads"], config_object["num_replicas"], "test_config.txt",
    config_object["system"], (config_object["num_replicas"] - 1)//3, numberOperations, client_id)
    print(cmd)
    exec_remote_cmd(client, cmd)

    # let the clients run
    clients_running = True
    while clients_running == True:
        clients_running = is_remote_process_running(client, "client")
        print "client running at host? ", client_id, ": ", clients_running
        sleep(10)

def calc_per_node_threads(total_threads, num_nodes, resources):
    total_resources = sum([resources[str(i)] for i in range(num_nodes)])
    per_node_threads = [0 for i in range(num_nodes)]
    for i in range(num_nodes):
        per_node_threads[i] += int((total_threads * resources[str(i)] + total_resources - 1)/ total_resources)
    delete_threads = sum(per_node_threads) - total_threads
    for i in range(delete_threads):
        per_node_threads[i] -= 1
    return per_node_threads

def experiment(config_object, step = 'all'):
    num_servers = config_object["num_servers"]
    servers = config_object["servers"]
    server_resources = config_object["server_resources"]
    num_clients = config_object["num_clients"]
    clients = config_object["clients"]
    client_resources = config_object["client_resources"]

    num_replicas = config_object["num_replicas"]
    num_client_threads = config_object["num_client_threads"]

    do_fresh_install = config_object["do_fresh_install"]
    network = config_object["network"]

    per_client_threads = calc_per_node_threads(num_client_threads, num_clients, client_resources)
    per_server_threads = calc_per_node_threads(num_replicas, num_servers, server_resources)
    print ("client threads:%r" % per_client_threads)
    print ("server threads:%r" % per_server_threads)
    # read servers
    serverips = []
    for i in range(num_servers):
        serverips.append(servers[str(i)])
    #serverips = list(set(serverips))
    print serverips

    # read clients
    clientips = []
    for i in range(num_clients):
        clientips.append(clients[str(i)])
    #clientips = list(set(clientips))
    print clientips

    if step.find('all') >= 0 or step.find('init') >= 0:
        # build keys and configs for servers
        setup_experiment_config(serverips, clientips, num_replicas, per_server_threads, per_client_threads)

        thread_list = []
        for host in serverips + clientips:
            ssh_client = setup_sshclient(host)
            t = threading.Thread(target = setup_remote_env, args = (ssh_client, host, do_fresh_install, network,) )
            thread_list.append(t)
        for thread in thread_list:
            thread.start()
        for thread in thread_list:
            thread.join()

    # Run experiment
    client_handlers = []
    if step.find('all') >= 0 or step.find('run_servers') >= 0:
        # Start servers
        nodeid = 0
        thread_list = []
        for host, num_replicas_per_server in zip(serverips, per_server_threads):
            for _ in range(num_replicas_per_server):
                client = setup_sshclient(host)
                t = threading.Thread(target = run_experiment_server, args = (nodeid, client, config_object) )
                thread_list.append(t)
                client_handlers.append(client)
                nodeid += 1

        for thread in thread_list:
            thread.start()

        print "waiting for servers to start"
        sleep(60)

    if step.find('all') >= 0 or step.find('run_clients') >= 0:
        nodeid = num_replicas
        thread_list = []
        for host, num_client_threads_per_client in zip(clientips, per_client_threads):
            for _ in range(num_client_threads_per_client):
                client = setup_sshclient(host)
                t = threading.Thread(target = run_experiment_client, args = (nodeid, client, config_object) )
                thread_list.append(t)
                client_handlers.append(client)
                nodeid += 1

        for thread in thread_list:
            thread.start()

        for thread in thread_list:
            thread.join()

    if step.find('all') >= 0 or step.find("copy") >= 0:
        data_dir = setup_local_logs()
        for i in range(num_servers):
            client = setup_sshclient(servers[str(i)])
            clients_running = is_remote_process_running(client, "server")
            print "server ", i, " running at host? ", servers[str(i)], ": ", clients_running

        # copy over logs from host
        for host in serverips + clientips:
            exec_local_cmd("scp %s:%s %s" % (host, "/dev/shm/*.log", data_dir))

        # write experiment config among log files
        config_object["data_dir"] = data_dir
        with open(data_dir + "/config.json", "w+") as cfg_file:
            json.dump(config_object, cfg_file, indent=1, sort_keys=True)

        # create a symlink to data dir
        os.system("rm -f latest_rslt; ln -sf %s latest_rslt" % (data_dir))

    if step.find('all') >= 0 or step.find("kill") >= 0:
        for host in serverips + clientips:
            ssh_client = setup_sshclient(host)
            print "Tearing down remote " + str(host)
            teardown_remote_env(ssh_client)

if __name__ == "__main__":
    parser = OptionParser()
    (options, args) = parser.parse_args()
    print (args[0])
    with open(args[0]) as config_file:
        config_object = json.load(config_file)

    step = args[1] if len(args) > 1 else 'all'
    experiment(config_object, step)

