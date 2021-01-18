STEP1: Generate the message definition file i.e.kv_pb2.py from kv.proto and keep in the same directory.

STEP2: Create a text file which contains information about the replicas ie replicas.txt

STEP3: Start the all Replicas by typing the following command: python3 replica.py <replica name> <port_no> <replicas.txt>

Example(4 Replicas):

python3 replica.py replica0 9091 replicas.txt

python3 replica.py replica1 9092 replicas.txt

python3 replica.py replica2 9093 replicas.txt

python3 replica.py replica3 9094 replicas.txt

Run the client: python client.py <ip_addr> <port>

Example : python3 client.py 128.226.114.201 9091
