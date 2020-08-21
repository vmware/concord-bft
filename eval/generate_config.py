import sys

def generate_config(file_prefix, algo, num_replica_servers, num_replica_threads, num_client_servers, num_client_threads, fresh_install):
    content = '''{
    "num_servers": %s,
    "servers": {
        "0": "10.0.0.4",
        "1": "10.0.0.5",
        "2": "10.0.0.6",
        "3": "10.0.0.7",
        "4": "10.0.0.13",
        "5": "10.0.0.14",
        "6": "10.0.0.15",
        "7": "10.0.0.16",
        "8": "10.0.0.17",
        "9": "10.0.0.18",
        "10": "10.0.0.19",
        "11": "10.0.0.20",
        "12": "10.0.0.21",
        "13": "10.0.0.22",
        "14": "10.0.0.23",
        "15": "10.0.0.24"
    },
    "server_resources": {
        "0": 4,
        "1": 4,
        "2": 4,
        "3": 4,
        "4": 4,
        "5": 4,
        "6": 4,
        "7": 4,
        "8": 4,
        "9": 4,
        "10": 4,
        "11": 4,
        "12": 4,
        "13": 4,
        "14": 4,
        "15": 4
    },
    "num_replicas": %s,
    "num_clients": %s,
    "clients": {
        "0": "10.0.0.8",
        "1": "10.0.0.10",
        "2": "10.0.0.11",
        "3": "10.0.0.12",
        "4": "10.0.0.13",
        "5": "10.0.0.14",
        "6": "10.0.0.15",
        "7": "10.0.0.16",
        "8": "10.0.0.17",
        "9": "10.0.0.18",
        "10": "10.0.0.19",
        "11": "10.0.0.20",
        "12": "10.0.0.21",
        "13": "10.0.0.22",
        "14": "10.0.0.23",
        "15": "10.0.0.24"
    },
    "client_resources": {
        "0": 4,
        "1": 4,
        "2": 4,
        "3": 4,
        "4": 4,
        "5": 4,
        "6": 4,
        "7": 4,
        "8": 4,
        "9": 4,
        "10": 4,
        "11": 4,
        "12": 4,
        "13": 4,
        "14": 4,
        "15": 4
    },
    "num_client_threads": %s,
    "do_fresh_install": %s,
    "system" : "%s",
    "exp_name": "arch_throughput",
    "network": "tcp"
}
    ''' % (num_replica_servers, num_replica_threads, num_client_servers, num_client_threads, fresh_install, algo)

    f = open("%s_servers_%s_clients_%s_%s.json" % (file_prefix, num_replica_threads, num_client_threads, algo), 'w')
    f.write(content)
    f.close()

if __name__ == '__main__':
    if len(sys.argv) < 8:
        print ("Usage: generate_config.py file_prefix algo num_replica_servers, num_replica_threads, num_client_servers, num_client_threads, fresh_install")
        exit(1)

    generate_config(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])
