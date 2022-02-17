import io
from operator import sub
import subprocess
import sys

def get_container_id():
    cmds = ['docker ps']

    try:
        p = subprocess.Popen(cmds, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        print(e.output)
        return 0

    if tuple(p.stderr):
        for line in p.stderr:
            print(line)
        return 0

    id = 0;
    for count, line in enumerate(p.stdout):
        print (count, line)
        if line.find(b'concord-bft') > 0:
            # find container
            id = line.split()[0]
            print('returned container id: {}'.format(id))
            return id
    return 0

def start_capture():

    print("Enter container names")
    print("1. concord-bft")
    print("2. Not implemented.........")

    opt = int(input("Enter option number: "))
    if (opt == 1):
        container_id = 0
        while container_id == 0:
            container_id = get_container_id()
            print("CID:", container_id)
        
        print("Container found, start capturing traffic")
        with open("/home/ssitu/b_pro/output2.log", "a") as output:
            subprocess.run("docker run --rm --net=container:concord-bft -v $PWD/tcpdump:/tcpdump tcpdump", shell=True, stdout=output, stderr=output, cwd='/home/ssitu/b_pro')

start_capture()