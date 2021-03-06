// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        v3.14.0
// source: examples/kafka_outbox_consume/hello/hello.proto

// To generate golang run:
// protoc -I=. --go_out=. ./examples/kafka_outbox_consume/hello/hello.proto

package hello

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type HelloPayload struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Age  int32  `protobuf:"varint,2,opt,name=age,proto3" json:"age,omitempty"`
}

func (x *HelloPayload) Reset() {
	*x = HelloPayload{}
	if protoimpl.UnsafeEnabled {
		mi := &file_examples_kafka_outbox_consume_hello_hello_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *HelloPayload) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HelloPayload) ProtoMessage() {}

func (x *HelloPayload) ProtoReflect() protoreflect.Message {
	mi := &file_examples_kafka_outbox_consume_hello_hello_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HelloPayload.ProtoReflect.Descriptor instead.
func (*HelloPayload) Descriptor() ([]byte, []int) {
	return file_examples_kafka_outbox_consume_hello_hello_proto_rawDescGZIP(), []int{0}
}

func (x *HelloPayload) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *HelloPayload) GetAge() int32 {
	if x != nil {
		return x.Age
	}
	return 0
}

var File_examples_kafka_outbox_consume_hello_hello_proto protoreflect.FileDescriptor

var file_examples_kafka_outbox_consume_hello_hello_proto_rawDesc = []byte{
	0x0a, 0x2f, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x73, 0x2f, 0x6b, 0x61, 0x66, 0x6b, 0x61,
	0x5f, 0x6f, 0x75, 0x74, 0x62, 0x6f, 0x78, 0x5f, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x2f,
	0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2f, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x12, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x22, 0x34, 0x0a, 0x0c, 0x48, 0x65, 0x6c, 0x6c,
	0x6f, 0x50, 0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03,
	0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x03, 0x61, 0x67, 0x65, 0x42, 0x2b,
	0x5a, 0x29, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x73, 0x2f, 0x6b, 0x61, 0x66, 0x6b, 0x61,
	0x5f, 0x6f, 0x75, 0x74, 0x62, 0x6f, 0x78, 0x5f, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x2f,
	0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x3b, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_examples_kafka_outbox_consume_hello_hello_proto_rawDescOnce sync.Once
	file_examples_kafka_outbox_consume_hello_hello_proto_rawDescData = file_examples_kafka_outbox_consume_hello_hello_proto_rawDesc
)

func file_examples_kafka_outbox_consume_hello_hello_proto_rawDescGZIP() []byte {
	file_examples_kafka_outbox_consume_hello_hello_proto_rawDescOnce.Do(func() {
		file_examples_kafka_outbox_consume_hello_hello_proto_rawDescData = protoimpl.X.CompressGZIP(file_examples_kafka_outbox_consume_hello_hello_proto_rawDescData)
	})
	return file_examples_kafka_outbox_consume_hello_hello_proto_rawDescData
}

var file_examples_kafka_outbox_consume_hello_hello_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_examples_kafka_outbox_consume_hello_hello_proto_goTypes = []interface{}{
	(*HelloPayload)(nil), // 0: hello.HelloPayload
}
var file_examples_kafka_outbox_consume_hello_hello_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_examples_kafka_outbox_consume_hello_hello_proto_init() }
func file_examples_kafka_outbox_consume_hello_hello_proto_init() {
	if File_examples_kafka_outbox_consume_hello_hello_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_examples_kafka_outbox_consume_hello_hello_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*HelloPayload); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_examples_kafka_outbox_consume_hello_hello_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_examples_kafka_outbox_consume_hello_hello_proto_goTypes,
		DependencyIndexes: file_examples_kafka_outbox_consume_hello_hello_proto_depIdxs,
		MessageInfos:      file_examples_kafka_outbox_consume_hello_hello_proto_msgTypes,
	}.Build()
	File_examples_kafka_outbox_consume_hello_hello_proto = out.File
	file_examples_kafka_outbox_consume_hello_hello_proto_rawDesc = nil
	file_examples_kafka_outbox_consume_hello_hello_proto_goTypes = nil
	file_examples_kafka_outbox_consume_hello_hello_proto_depIdxs = nil
}
