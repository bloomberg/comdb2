#!/usr/bin/python
import socket
import struct
import sys
try:
  import sqlquery_pb2
  import sqlresponse_pb2
except ImportError:
   raise ImportError('Install python-protobuf and run make')

def portmux_get(host, dbname):
  client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  service = "comdb2/replication/"
  command = "rte " + service + dbname  +"\n"
  client_socket.connect((host, 5105))
  client_socket.send(command)
  byts = client_socket.recv(32)
  port = int(byts)
  return client_socket
  

if len(sys.argv) != 4:
    print "Run with the following arguments"
    print sys.argv[0] + " <dbname> <machine> <query>"
    sys.exit()
    

dbname  = sys.argv[1]
machine = sys.argv[2]
client_query   = sys.argv[3]


client_socket =  portmux_get(machine, dbname)
client_socket.send("newsql\n")


query = sqlquery_pb2.CDB2_QUERY()
query.sqlquery.sql_query = client_query;
query.sqlquery.dbname = dbname
query.sqlquery.little_endian  = 0
query.sqlquery.tzname = "America/New_York"

size = query.ByteSize();
send_data = query.SerializeToString();

byts = struct.pack("!iiii",sqlquery_pb2.CDB2QUERY,0,0,size,)
client_socket.send(byts) #send newsql header
client_socket.send(send_data)

firstresponse = sqlresponse_pb2.CDB2_SQLRESPONSE()
byts = client_socket.recv(16) #read newsql header
if (struct.unpack("!iiii",byts)[0] != sqlresponse_pb2.SQL_RESPONSE):
    sys.stdout.write("Invalid reply from server.\n")
    sys.exit()
size = (struct.unpack("!iiii",byts))[3]
firstresponse.ParseFromString(client_socket.recv(size))
if (firstresponse.response_type != sqlresponse_pb2.COLUMN_NAMES):
    sys.stdout.write("Invalid reply from server.\n")
    sys.exit()
if (firstresponse.error_code != sqlresponse_pb2.OK):
    sys.stdout.write("Error: rc="+str(firstresponse.error_code)+ " str="+firstresponse.error_string+"\n")
    sys.exit()

#get first column value
response = sqlresponse_pb2.CDB2_SQLRESPONSE()
byts = client_socket.recv(16) #read newsql header
if (struct.unpack("!iiii",byts)[0] != sqlresponse_pb2.SQL_RESPONSE):
    sys.exit("Invalid reply from server.")
size = (struct.unpack("!iiii",byts))[3] 
response.ParseFromString(client_socket.recv(size))

while response.response_type == sqlresponse_pb2.COLUMN_VALUES:
  if (response.error_code != sqlresponse_pb2.OK):
    sys.stdout.write("Error: RC="+str(response.error_code)+ " str="+response.error_string+"\n")
    sys.exit()
  i = 0
  sys.stdout.write("(")
  for column in response.value:
       if (i > 0) : 
         sys.stdout.write(", ")
       sys.stdout.write(firstresponse.value[i].value)
       sys.stdout.write("=")
       if (column.isnull== True):
         sys.stdout.write("NULL")
       elif (firstresponse.value[i].type == sqlresponse_pb2.INTEGER):
         vallong = struct.unpack("!q", column.value)[0]
         sys.stdout.write(str(vallong))
       elif (firstresponse.value[i].type == sqlresponse_pb2.REAL):
         valreal = struct.unpack("!d", column.value)[0]
         sys.stdout.write(str(valreal))
       elif (firstresponse.value[i].type == sqlresponse_pb2.DATETIME):
         datetime = struct.unpack("!iiiiiiiiiI36s", column.value)
         sys.stdout.write("\""+str(datetime[5]+1900)+"-"+str(datetime[4]+1)+"-"+
                          str(datetime[3])+"T"+str(datetime[2]).zfill(2)+str(datetime[1]).zfill(2)+str(datetime[0]).zfill(2)+"."+str(datetime[9]).zfill(3)+
                          " " +datetime[10]+"\"")
       elif (firstresponse.value[i].type == sqlresponse_pb2.DATETIMEUS):
         datetime = struct.unpack("!iiiiiiiiiI36s", column.value)
         sys.stdout.write("\""+str(datetime[5]+1900)+"-"+str(datetime[4]+1)+"-"+
                          str(datetime[3])+"T"+str(datetime[2]).zfill(2)+str(datetime[1]).zfill(2)+str(datetime[0]).zfill(2)+"."+str(datetime[9]).zfill(6)+
                          " " +datetime[10]+"\"")
       elif (firstresponse.value[i].type == sqlresponse_pb2.INTERVALDS):
         intervalds = struct.unpack("!iIIIII", column.value)
         sign = ""
         if (intervalds[0] < 0):
             sign = "-"
         sys.stdout.write("\""+sign+ str(intervalds[1]) + " " +
                          str(intervalds[2])+":"+str(intervalds[3])+":"+str(intervalds[4])+"."+str(intervalds[5]).zfill(3)+"\"")
       elif (firstresponse.value[i].type == sqlresponse_pb2.INTERVALDSUS):
         intervalds = struct.unpack("!iIIIII", column.value)
         sign = ""
         if (intervalds[0] < 0):
             sign = "-"
         sys.stdout.write("\""+sign+ str(intervalds[1]) + " " +
                          str(intervalds[2])+":"+str(intervalds[3])+":"+str(intervalds[4])+"."+str(intervalds[5]).zfill(6)+"\"")         
       elif (firstresponse.value[i].type == sqlresponse_pb2.BLOB):
         sys.stdout.write("x'")
         sys.stdout.write(column.value.encode("hex"))
         sys.stdout.write("'")
       else:    
         sys.stdout.write("'")
         sys.stdout.write(column.value)
         sys.stdout.write("'")
       i = i + 1
  size = 0     
  while (size == 0):
    byts = client_socket.recv(16) #read newsql header
    size = (struct.unpack("!iiii",byts))[3] #newsql header with no data is server heartbeat
  response.ParseFromString(client_socket.recv(size))
  sys.stdout.write(")\n")
