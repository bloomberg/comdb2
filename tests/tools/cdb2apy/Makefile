all: sqlquery sqlresponse

sqlquery:
	protoc --python_out=. -I../../../protobuf ../../../protobuf/sqlquery.proto

sqlresponse:
	protoc --python_out=. -I../../../protobuf ../../../protobuf/sqlresponse.proto

clean:
	rm -f *.pyc
	rm -f *_pb2.py
