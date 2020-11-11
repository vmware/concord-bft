import socket
import json
import struct
import time
import glob

ENDPOINTS = [("rep", "localhost", 4710, "lastStableSeqNum"), ("ror", "localhost", 4718, "lastExecutedSeqNum")]
S3_DATA_DIR = "./s3-data/blockchain/"
MESSAGE = b'\x00\x00\x00\x00\x00\x00\x00\x00\x01'
HEADER_FMT = "<BQ"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
cnt = 1
while True:
    for e in ENDPOINTS:
        sock.sendto(MESSAGE, (e[1], e[2]))
        resp, addr = sock.recvfrom(4096) # buffer size is 1024 bytes
        data = json.loads(resp[HEADER_SIZE:])

        for component in data['Components']:
            if component['Name'] == "replica":
                print("%d: %s: %s: %s" % (cnt, e[0], e[3], component["Gauges"][e[3]]))

    print("%d: blocks on s3: %d" % (cnt, len(glob.glob(S3_DATA_DIR + '*/*'))))
    time.sleep(1)
    cnt = cnt+1
    print("\n")