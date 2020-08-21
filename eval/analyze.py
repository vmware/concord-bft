import os, errno
import subprocess
import base64
import datetime
import pwd
import json
import sys
from optparse import OptionParser

def analyze_archipelago(system_type, data_dir, num_replicas, num_clients):
    rslt_object = {}

    if system_type == 'archipelago':
        ll = []
        for j in range(0, num_replicas):
            replica_log = '%s/replica%d.log' % (data_dir, j)
            res = subprocess.check_output("cat %s | grep TotalOrderCommit | awk \'{split($8, s, \"=\");print s[2];}\'" % (replica_log), shell = True).rstrip('\n').split('\n')
            if len(res) < 2: continue
            res = [float(v) for v in res]
            res.sort()
            print ("server%d avg:%f 50:%f 90:%f 99:%f" % (j, sum(res)/len(res), res[len(res)//2], res[len(res)*9//10], res[len(res)*99//100]))
            ll += res
        if len(ll) > 0:
            ll.sort()
            rslt_object['avg_totalorder_latency'] = sum(ll) / len(ll)
            rslt_object['50_totalorder_latency'] = ll[len(ll)//2]
            rslt_object['90_totalorder_latency'] = ll[len(ll)*9//10]
            rslt_object['99_totalorder_latency'] = ll[len(ll)*99//100]
        else:
            rslt_object['avg_totalorder_latency'] = 0
            rslt_object['50_totalorder_latency'] = 0
            rslt_object['90_totalorder_latency'] = 0
            rslt_object['99_totalorder_latency'] = 0

    print ('calc avg_latency...')
    ll = []
    for j in range(num_replicas, num_replicas + num_clients):
        client_log = '%s/client%d.log' % (data_dir, j)
        res = subprocess.check_output("cat %s | grep duration | awk \'{split($16, s, \"=\");split(s[2], t, \")\");print t[1];}\'" % (client_log), shell = True).rstrip('\n').split('\n')
        res = [float(v) for v in res]
        res.sort()
        print ("client%d avg:%f 50:%f 90:%f 99:%f" % (j, sum(res)/len(res), res[len(res)//2], res[len(res)*9//10], res[len(res)*99//100]))
        ll += res
    if len(ll) > 0:
        ll.sort()
        rslt_object['avg_latency'] = sum(ll) / len(ll)
        rslt_object['50_latency'] = ll[len(ll)//2]
        rslt_object['90_latency'] = ll[len(ll)*9//10]
        rslt_object['99_latency'] = ll[len(ll)*99//100]
    else:
        rslt_object['avg_latency'] = 0
        rslt_object['50_latency'] = 0
        rslt_object['90_latency'] = 0
        rslt_object['99_latency'] = 0

    print ('calc throughput...')
    begin_time = None
    end_time = None
    for j in range(num_replicas, num_replicas + num_clients):
        client_log = '%s/client%d.log' % (data_dir, j)
        begin = subprocess.check_output("cat %s | grep Starting | head -n 1 | awk \'{print $3}\'" %(client_log), shell = True).rstrip('\n')
        end = subprocess.check_output("cat %s | grep INFO | tail -n 1 | awk \'{print $3}\'" %(client_log), shell = True).rstrip('\n')
        print ('client%d %s %s' % (j, str(begin), str(end)))
        beginT = datetime.datetime.strptime(str(begin), '%H:%M:%S.%f')
        endT = datetime.datetime.strptime(str(end), '%H:%M:%S.%f')

        if begin_time is None or begin_time > beginT:
            begin_time = beginT
        if end_time is None or end_time < endT:
            end_time = endT

    duration = float((end_time - begin_time).total_seconds())
    rslt_object['throughput'] = float(2800.0 * num_clients // duration) # query per second
    print (rslt_object)
    return rslt_object

def analyze(config_object):
    num_replicas = config_object["num_replicas"]
    num_clients = config_object["num_client_threads"]
    data_dir = config_object["data_dir"]

    system_type = config_object["system"]
    experiment_name = config_object["exp_name"]

    # call the experiment specific analyze function
    rslt_object = {}
    rslt_object = analyze_archipelago(system_type, data_dir, num_replicas, num_clients)

    # dump processed results and config
    rslt_object["exp_config"] = config_object
    with open(data_dir + "/rslt.json", "w+") as rslt_file:
        json.dump(rslt_object, rslt_file, indent=1, sort_keys=True)

if __name__ == "__main__":
    parser = OptionParser()
    (options, args) = parser.parse_args()
    with open(args[0]) as config_file:
        config_object = json.load(config_file)

    analyze(config_object)
