import pyshark
from enum import IntEnum

import json

class Msgs(IntEnum):
    PrePrepare = 100
    PreparePartial = 101
    PrepareFull = 102
    CommitPartial = 103
    CommitFull = 104
    StartSlowCommit = 105
    PartialCommitProof = 106
    FullCommitProof = 107
    PartialExecProof = 108
    FullExecProof = 109
    SimpleAck = 110
    ViewChange = 111
    NewView = 112
    Checkpoint = 113
    AskForCheckpoint = 114
    ReplicaStatus = 115
    ReqMissingData = 116
    StateTransfer = 117
    ReplicaAsksToLeaveView = 118
    ReplicaRestartReady = 119
    ReplicasRestartReadyProof = 120

    ClientPreProcessRequest = 500
    PreProcessRequest = 502
    PreProcessReply = 503
    PreProcessBatchRequest = 504
    PreProcessBatchReply = 505
    PreProcessResult = 506

    ClientRequest = 700
    ClientBatchRequest = 750
    ClientReply = 800

f = open('config.json')
data = json.load(f)

msg_code = 0
file1 = open('decodefile.txt', 'w')
protocol = 0

def print_callback(packet):
    layer = str(packet.layers)
    if msg_code == 0:
        if protocol == 1:
            if "UDP" in layer:
                file1.write("=============\n")
                file1.write(str(packet))
        elif protocol == 2:
            if "TCP" in layer:
                file1.write("=============\n")
                file1.write(str(packet))
        elif protocol == 3:
            if "SSL" in layer:
                file1.write("=============\n")
                file1.write(str(packet))
        else:
            file1.write("=============\n")
            file1.write(str(packet))
    else:
        print("MSg codes decode")
        print("Not implemented")
        return
        #payload = packet.data.data
        #print(payload)

def print_packet(cap):
    try:
        cap.apply_on_packets(print_callback, timeout=10000)
    except:
        print("Done")
        return None

def print_decpak():
    #here we can use the json inputs from config file
    capture = pyshark.FileCapture('tcpdump/tcpdump.pcap',
                                    display_filter='ip.src == 127.0.0.1',
                                    override_prefs={'ssl.keys_list':'127.0.0.1,4443,http,server.pem'},
                                    debug=True)
    print("HI")
    for packet in capture:
        print (packet)
        if "DATA-TEXT-LINES" in packet:

            #print(packet.layers)
            print(packet['DATA-TEXT-LINES'])

        else:
            print("Could not decrypt data")

print("Select options to get packets from pcap file")
print("1. Msg codes")
print("2. Decode with protocol")
print("3. Decode pcap file")

a = int(input("Enter option: "))

if a==1:
    print({i.name: i.value for i in Msgs})
    msg_code = int(input("Enter msg code: "))
    cap = pyshark.FileCapture("tcpdump/tcpdump.pcap")
    print_packet(cap)
if a==2:
    print("Protocols")
    print("1. UDP")
    print("2. TCP")
    print("3. SSL")
    print("4. SSL with decryption")
    protocol = int(input("Enter value: "))
    if (protocol != 4):
        cap = pyshark.FileCapture("tcpdump/tcpdump.pcap")
        print("Writing packet details to file")
        print_packet(cap)
    else:
        print_decpak()
else:
    cap = pyshark.FileCapture("tcpdump/tcpdump.pcap")
    print("Writing packet details to file")
    print_packet(cap)

file1.close()
