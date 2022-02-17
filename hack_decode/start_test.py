
import io
import subprocess
import sys

def start_tests():
    with open("/home/ssitu/b_pro/output1.log", "+w") as output:
        subprocess.run("make test", shell=True, stdout=output, stderr=output, cwd='/home/ssitu/wrk_bft_client/concord-bft')

start_tests()

