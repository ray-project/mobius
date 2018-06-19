# Generate pb file for Python RPC
python -m grpc_tools.protoc -Iproto -I/usr/local/include -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis  --python_out=proto --grpc_python_out=proto mobius.proto

# Generate pb file for Go gateway
protoc -I/usr/local/include -Iproto -I$GOPATH/src -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --grpc-gateway_out=logtostderr=true:go_gateway mobius.proto
# Generate pb file for Go RPC
protoc -I/usr/local/include -Iproto -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis --go_out=plugins=grpc:go_gateway mobius.proto
