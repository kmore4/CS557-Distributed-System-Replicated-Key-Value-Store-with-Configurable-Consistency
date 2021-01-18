'''
    @Author: Sonu Gupta
    @Purpose: This file acts as a 'Client'
'''
import logging
import logging.config
import json
import sys
sys.path.append('/home1/kmore4/protobuf/python')
import os
import socket
import kv_pb2
import random
from random import randint
import time
import struct

# globals
channels = zip()
br_name = ""
branch_info = []
BUFFER_SIZE = 1024
branch_dict = {}
REPLICA_FILE = 'replicas.txt'


def parseFile(filename):
        global branch_info
        global branch_dict
        branch_dict = {}
        branch_info = []
        f = open(filename)
        values = f.read().split()
        i=0
        while i != len(values):
            branch_info.append(values[i+1])
            branch_info.append(values[i+2])
            branch_dict[str(values[i])] = branch_info
            branch_info = [] # since, added to dictionary, reset it otherwise old list will be appended.
            i = i+3

        f.close()


        return branch_dict

def sendDataOverSocket(ip, port, message):

    sock = socket.socket()
    sock.connect((ip, int(port))) # Connect to branch and send message.
    sock.sendall(message.SerializeToString()) # .encode('ascii')
    data = sock.recv(BUFFER_SIZE)
    kv_message = kv_pb2.KVMessage()
    kv_message.ParseFromString(data)

    ## Since, co-ordinator will response either True or False and based on that client will just print
    ## o/p on console
    if kv_message.HasField('cord_response'):
        ## check status
        if kv_message.cord_response.status is True:
            print('=====================================================================')
            print('Operation Successfully Completed....!!!')
            print(' Key: ' + str(kv_message.cord_response.key))
            print(' Value: ' + kv_message.cord_response.value)
            print('=====================================================================')
            print()
        else:
            print('=====================================================================')
            print("Error: Either Key is not Valid or Some error in writing key to DB.")
            print('=====================================================================')

    elif kv_message.HasField('error_message'):
            print('=====================================================================')
            print("Error:" + str(kv_message.error_message.msg))
            print('=====================================================================')

    return kv_message.ParseFromString(data)

def get_request(key, c_level):
    # creating probuf based message here and returning to send over socket
    kv_message = kv_pb2.KVMessage()
    get_request = kv_message.get_request
    get_request.key = int(key)
    get_request.consistency_level = int(c_level)

    return kv_message

def put_request(key, value, c_level):
    # creating probuf based message here and returning to send over socket
    kv_message = kv_pb2.KVMessage()
    put_request = kv_message.put_request
    put_request.key = int(key)
    put_request.value = value
    put_request.consistency_level = int(c_level)

    return kv_message


def main():
    # Controller will parse the text file and fill branch info in local structure

    try:
        ## Create socket to send requests to 'branch'.
        # sock = socket.socket()

        if len(sys.argv)  != 3:
            print('Invalid parameters passed.')

        ## parse file
        replica_dict =  parseFile(REPLICA_FILE)

        ## Display all entity involved here.

        ## Extract co-ordinators ip-port here.
        ip = sys.argv[1]
        port = int(sys.argv[2])

        while True:

            print('Enter the action to perform:')
            print('1. Get Key ')
            print('2. Put Key ')
            print('3. Exit ')
            choice = input()
        #####  GET REQUEST
            if int(choice) == 1:
                # call get routine
                try:
                    print('Enter the key to search')
                    key = input()
                    if 0<=int(key)<=255:
                        print('Enter the CONSISTENCY LEVEL you would like.')
                        print('1. ONE')
                        print('2. QUORUM')
                        c_level = input()
                        req = get_request(key, c_level)
                        response = sendDataOverSocket(ip, port, req)
                    else:
                        print('Key if Out-of Range. Please Enter between range 0-255')
                except ValueError:
                    print('Error: Please Enter Valid \'Integer Value\' ')
                except:
                    print('Error: Internal Error Occured')

        #####  PUT REQUEST

            elif int(choice) == 2:
                # call put routine
                try:
                    print('Enter the key  ')
                    key = input()
                    if 0<=int(key)<=255:
                        print('Enter the value  ')
                        value = input()
                        print('Enter the CONSISTENCY LEVEL you would like. ')
                        print('  1. ONE')
                        print('  2. QUORUM')
                        c_level = input()
                        req = put_request(key, value, c_level)
                        response = sendDataOverSocket(ip, port, req)
                    else:
                        print('Key if Out-of Range. Please Enter between range 0-255')
                except ValueError:
                    print('Error: Please Enter Valid \'Integer Value\' ')
                except:
                    print('Error: Internal Error Occured')

        ####  QUIT
            elif int(choice) == 3:
                exit(0)
                # call exit routine here
            else:
                print('Invalid Choice. Exiting..!!')

            ## Build messages get/put based on user input.

    except KeyboardInterrupt:
        print('Ctr + C is pressed exiting controller')
    finally:
        print('main() exit')

# Main routine
if __name__ == '__main__':

    main()
