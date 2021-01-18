
#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import logging.config
import ast
import re
import os
import sys
import json
import socket
import threading
import time
import operator
import random
sys.path.append('/home1/kmore4/protobuf/python')
import kv_pb2
from threading import Lock
from pathlib import Path

KV_FILE = ""
replica_dict = {}
replica_names_list = []
other_replicas_dict = {}
# Locking Variable
lock = Lock()
ip_to_replica = {}
key_value_store = {}
my_name = ''
timestamp = 0.0
read_counter = 0
write_counter = 0
hinted_list = []
CO_ORDINATOR = 1
REPLICA = 2
GET = 0
PUT = 1
CONSISTENCY_LEVEL_QUORUM = 2
CONSISTENCY_LEVEL_ONE = 1
HINTED_HANDOFF = 2
configuration = 0
hint_dict = {}

def parseFile(filename):
    replica_dict = {}
    replica_info = []
    f = open(filename)
    values = f.read().split()

    for i in range(0,len(values),3):
        v = values[i]
        if v:
            str_v = str(v)
        v1 = values[i+1]
        v2 = values[i+2]
        if v2:
            str_v2 = str(v2)
        if v1:
            replica_info.append(values[i+1])
        if v2:
            replica_info.append(values[i+2])
        replica_dict[str_v] = replica_info

        if str_v == sys.argv[1]:
            pass
        else:
            replica_names_list.append(str_v)

        ## ip->port Map
        ip_port = v1 +':'+ v2
        ip_to_replica[ip_port]= v

        replica_info = []

    f.close()

    return replica_dict

def InitializeDataStructures():
    global replica_dict
    global other_replicas_dict

    replica_dict =  parseFile(sys.argv[3])
    other_replicas_dict = replica_dict.copy()
    other_replicas_dict.pop(sys.argv[1], None)

    ########## creating the hint_dictionary data structure  ####
    for key in other_replicas_dict.keys():
        hint_dict[getReplicaIDFromName(key)] = []

def InitializeKVStore():
    global KV_FILE
    global key_value_store

    log_file = Path(KV_FILE)

    try:
        if log_file.is_file():
            with open(KV_FILE, 'r') as f:
                s = f.read()
                key_value_store = ast.literal_eval(s)
        else:
            with open(KV_FILE, 'w') as f:
                key_value_store = {}
                s = f.write(str(key_value_store))
    except SyntaxError:
        print('Error: Please DELETE old DB file.')
    except ValueError:
        print('Error: Please DELETE old DB file.')
    except:
        print('Some Internal Error Occured while initilising KV STORE')

def getCurentReplicaID():
    s = [my_name]
    ID = re.findall('\d+', s[0])

    return ID[0]

def getReplicaIDFromName(r_name):

    s = [r_name]
    ID = re.findall('\d+', s[0])

    return ID[0]


def getReplicaList(key):

    if 0<=int(key)<=63:
        return ['0','1','2']
    if 64<=int(key)<=127:
        return ['1','2','3']
    if 128<=int(key)<=191:
        return ['2','3','0']
    if 192<=int(key)<=255:
        return ['3','0','1']
    return []

def AmIReplica(partioner_ring):

    ID = getCurentReplicaID()

    if ID not in partioner_ring:
        return False
    return True

def update_key_value_store(key, value, p_timestamp):
        global key_value_store
        global KV_FILE
        global timestamp

        write_timestamp = p_timestamp

        partioner_list = getReplicaList(key)
        does_key_belong_here = AmIReplica(partioner_list)

        if does_key_belong_here:
            try:
                if str(key) not in key_value_store or key_value_store[str(key)][1] < write_timestamp:
                    lock.acquire()
                    key_value_store[str(key)] = [value, write_timestamp]
                    lock.release()

                with open(KV_FILE, "w") as f:
                    f.write(str(key_value_store))
                    f.close()
            except:
                return False

        return True

def send_ack_to_client(client_conn, request):

        response_kv_message = kv_pb2.KVMessage()
        cord_response = response_kv_message.cord_response
        cord_response.key = request.key
        cord_response.status = True
        cord_response.value = request.value

        client_conn.sendall(response_kv_message.SerializeToString())

def send_errmsg_to_client(client_conn, msg):

        response_kv_message = kv_pb2.KVMessage()
        error_message = response_kv_message.error_message
        error_message.msg = msg

        client_conn.sendall(response_kv_message.SerializeToString())

def makeTuple(ID, timestamp,key, value):
    return tuple((ID, key, value, timestamp))

def eventually_update_replicas(client_conn, partioner_list, request, approach, op_type):
    global write_counter
    global read_counter
    global hinted_list
    global timestamp
    global configuration
    global hint_dict

    kvlist_of_tuple = []
    absence_count = 0

    if op_type == GET:
        keycheck = str(request.key)
        if AmIReplica(partioner_list) and keycheck in key_value_store:
            my_data = key_value_store[keycheck]
            my_id = getCurentReplicaID()
            kvlist_of_tuple.append(tuple((my_id, keycheck, my_data[0], my_data[1])))
        elif AmIReplica(partioner_list) and keycheck not in key_value_store:
            absence_count = absence_count + 1

    hinted_list = []

    try:

        response_kv_message = kv_pb2.KVMessage()
        replica_request = response_kv_message.replica_request
        replica_request.key = request.key
        replica_request.id = request.id

        if op_type == PUT:
            replica_request.value = request.value

        replica_request.timestamp = timestamp

        if op_type != PUT:
            replica_request.operation = GET
        elif op_type != GET:
            replica_request.operation = PUT

        for ID in partioner_list:
            if ID != getCurentReplicaID():
                replica_name = 'replica'+str(ID)
                replica_info = replica_dict[replica_name]

                try:
                    sock = socket.socket()
                    sock.connect((replica_info[0], int(replica_info[1]))) # Connect to branch and send message.
                    sock.sendall(response_kv_message.SerializeToString()) # .encode('ascii')
                    data = sock.recv(1024)
                    sock.close()
                except:
                    hinted_list.append(ID)
                    sock.close()

                    if op_type == PUT:
                        hint_dict[ID].append(tuple((request.key, request.value, timestamp)))

                    continue

                response_from_replica = kv_pb2.KVMessage()
                response_from_replica.ParseFromString(data)

                if response_from_replica.HasField('replica_response'):
                    replica_response = response_from_replica.replica_response


                    for hinted_handoff in replica_response.hinted_handoff:
                        update_key_value_store(hinted_handoff.key, hinted_handoff.value, hinted_handoff.timestamp)

                    if replica_response.status is True:                # Replica successfully updated

                        if op_type == PUT:

                            write_counter = write_counter + 1

                            if write_counter >= 2 and approach == CONSISTENCY_LEVEL_QUORUM:

                                update_key_value_store(request.key, request.value, timestamp) #Its co-ordinator who is updating
                                send_ack_to_client(client_conn, request)
                            elif write_counter == 1 and approach == CONSISTENCY_LEVEL_ONE:          # co-rdinator != replica [by client]
                                send_ack_to_client(client_conn, request)

                        elif op_type == GET:

                            read_counter = read_counter + 1
                            appendItem = makeTuple(ID, replica_response.timestamp,replica_response.key, replica_response.value)
                            kvlist_of_tuple.append(appendItem)

                            if read_counter == 1 and approach == CONSISTENCY_LEVEL_ONE:          # co-rdinator != replica [by client]
                                request.value = replica_response.value
                                send_ack_to_client(client_conn, request)

                    else:
                        if op_type == GET:
                            read_counter = read_counter + 1
                            absence_count = absence_count + 1

                        hinted_list.append(ID)
                        appendItem = makeTuple(ID, replica_response.timestamp,replica_response.key, replica_response.value)
                        kvlist_of_tuple.append(appendItem)

        if op_type == PUT:
            if approach == CONSISTENCY_LEVEL_QUORUM and write_counter < 2 :

                send_errmsg_to_client(client_conn, 'Not Enough Replica Available')

        if op_type == GET:

            if approach == CONSISTENCY_LEVEL_QUORUM and read_counter < 2:
                send_errmsg_to_client(client_conn, 'Error: Not Enough Replica Available')

            if read_counter >= 2 and approach == CONSISTENCY_LEVEL_QUORUM:

                if absence_count == 3:
                    send_errmsg_to_client(client_conn, 'Error: Key is NOT present')
                else:
                    kvlist_of_tuple.sort(key=operator.itemgetter(3), reverse=True) # sort in ascending
                    replica_id, key, value, timestamp = kvlist_of_tuple[0]

                    request.key = int(key)
                    request.value = value
                    send_ack_to_client(client_conn, request)

    except KeyError:
        print('Invalid Replica Name. Please check replicas.txt')

    if len(hinted_list) == 3:
        send_errmsg_to_client(client_conn, 'All replicas are down.')


def  handle_ONE_approach(client_conn, request, op_type):
    global write_counter
    global read_counter
    global timestamp

    partioner_list = getReplicaList(request.key)

    if AmIReplica(partioner_list) is True:
        if op_type == PUT:     # Write operation
            write_counter = write_counter + 1
            update_key_value_store(request.key, request.value, timestamp) #Its co-ordinator who is updating
        elif op_type == GET:   # Read operation
            if str(request.key) not in key_value_store:
                send_errmsg_to_client(client_conn, 'Key Does not exist')
                return False
            else:
                read_counter = read_counter + 1
                request.value = key_value_store[str(request.key)][0] ## Fill value here, so client can display

        send_ack_to_client(client_conn, request)

    eventually_update_replicas(client_conn, partioner_list, request, CONSISTENCY_LEVEL_ONE, op_type)

def handle_QUORUM_approach(client_conn, request, op_type): # QUORUM
    global write_counter
    global read_counter
    global timestamp

    partioner_list = getReplicaList(request.key)

    if AmIReplica(partioner_list):
        if op_type != PUT:
              read_counter = read_counter + 1
        elif op_type != GET:
            write_counter = write_counter + 1


    heartbeat_kv_message = kv_pb2.KVMessage()
    heart_beat = heartbeat_kv_message.heart_beat
    heart_beat.status = True  #dummy data. We dont care

    if op_type == PUT:
        replica_availability = write_counter
    elif op_type == GET:
        replica_availability = read_counter

    for ID in partioner_list:
        if ID != getCurentReplicaID():
            replica_name = 'replica'+str(ID)
            replica_info = replica_dict[replica_name]
            ip = replica_info[0]
            port = replica_info[1]

            try:
                sock = socket.socket()
                sock.connect((ip, int(port))) # Connect to branch and send message.
                sock.sendall(heart_beat.SerializeToString()) # .encode('ascii')
                sock.close()
                replica_availability = replica_availability + 1
            except:
                sock.close()
                continue

    if replica_availability >= 2:
        eventually_update_replicas(client_conn, partioner_list, request, CONSISTENCY_LEVEL_QUORUM, op_type)
    else:
        send_errmsg_to_client(client_conn, 'Not Enough Replica Available')

if __name__ == '__main__':
    if(len(sys.argv)!=4):
        print('Invalid arguments passed')
        sys.exit(0)


    ip = socket.gethostbyname(socket.getfqdn())
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((ip, int(sys.argv[2])))
    server_socket.listen(5)
    my_name = sys.argv[1]


    configuration = HINTED_HANDOFF

    ###
    InitializeDataStructures()

    KV_FILE = 'write-ahead{}.db'.format(getCurentReplicaID())

    InitializeKVStore()

    print('\n Listening on ' + str(ip) + ' : '+ sys.argv[2])
    print('\n Started Replica' + getCurentReplicaID() + '... ')
    try:
        while True:
            kv_message = kv_pb2.KVMessage()

            client_conn, client_addr = server_socket.accept()
            data = client_conn.recv(1024)  # dont print data directly, its binary
            kv_message.ParseFromString(data)

            client_ip, client_port = client_conn.getsockname()

            if kv_message.HasField('get_request'):

                read_counter = 0
                get_request = kv_message.get_request
                get_request.id = int(getCurentReplicaID())


                if get_request.consistency_level == 1:              # ONE
                    handle_ONE_approach(client_conn, get_request, GET)
                elif get_request.consistency_level == 2:
                    handle_QUORUM_approach(client_conn, get_request, GET) # QUORUM


            elif kv_message.HasField('put_request'):
                write_counter = 0

                timestamp = time.time()


                put_request = kv_message.put_request
                put_request.id = int(getCurentReplicaID())


                ## Check Consistency level:
                if put_request.consistency_level == 1:              # ONE
                    handle_ONE_approach(client_conn, put_request, PUT)
                elif put_request.consistency_level == 2:
                    handle_QUORUM_approach(client_conn, put_request, PUT) # QUORUM

            elif kv_message.HasField('replica_request'):
                current_id =  int(getCurentReplicaID())

                replica_request = kv_message.replica_request

                replica_response_message = kv_pb2.KVMessage()
                replica_response = replica_response_message.replica_response
                replica_response.id = current_id

                if str(replica_request.id) in hint_dict:

                    stale_data_list = hint_dict.get(str(replica_request.id))
                    for data in stale_data_list:

                        hh_message = replica_response.hinted_handoff.add()
                        hh_message.id = current_id
                        hh_message.operation = PUT
                        hh_message.key, hh_message.value, hh_message.timestamp = data # tuples will be extracted here

                if replica_request.operation == 0:              # READ
                    replica_response.key = replica_request.key

                    if str(replica_request.key) in key_value_store:
                        value = key_value_store.get(str(replica_request.key))  # Fetching value of key
                        replica_response.status = True
                        replica_response.value = value[0]
                        replica_response.timestamp = float(value[1])
                    else:
                        replica_response.status = False

                elif replica_request.operation == 1:              # WRITE
                    replica_response.key = replica_request.key
                    if update_key_value_store(replica_request.key, replica_request.value, replica_request.timestamp) == False:
                        replica_response.status = False
                    else:
                        replica_response.status = True

                client_conn.sendall(replica_response_message.SerializeToString())

                if str(replica_request.id) in hint_dict:
                    hint_dict[str(replica_request.id)] = []


            elif kv_message.HasField('display_kvstore'):

                print('\n--------------xxxxxxx---------------')
                print('REPLICA ' + getCurentReplicaID())
                print('Current State of HashTable:')
                print('{')
                for key in key_value_store:
                    print( '     ' + key + ':' + str(key_value_store[key]))
                print('}')
                print('--------------XXXXXXX---------------\n')

    except KeyboardInterrupt:
        print("\nServer Stopped\n")
    finally:
        server_socket.shutdown(socket.SHUT_RDWR)
        server_socket.close()
