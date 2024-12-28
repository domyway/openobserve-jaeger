package errors

import (
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoimpl"
)

const (
	UnknownCode = 500
)

type Error struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code     int32             `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	Reason   string            `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason,omitempty"`
	Message  string            `protobuf:"bytes,3,opt,name=message,proto3" json:"msg,omitempty"`
	Metadata map[string]string `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (e Error) Error() string {
	return e.Message
}

func (e Error) ErrorWithCode() string {
	return strconv.Itoa(int(e.Code)) + ": " + e.Message
}

func (e Error) Is(err error) bool {
	if se := new(Error); errors.As(err, &se) {
		return true
	}
	return false
}

func New(code int32, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

func Newf(code int32, format string, a ...interface{}) *Error {
	return New(code, fmt.Sprintf(format, a...))
}

func Errorf(code int32, format string, a ...interface{}) error {
	return New(code, fmt.Sprintf(format, a...))
}

func Code(err error) int {
	if err == nil {
		return 200
	}
	return int(FromError(err).Code)
}

func FromError(err error) *Error {
	if err == nil {
		return nil
	}
	if se := new(Error); errors.As(err, &se) {
		return se
	}

	gs, ok := status.FromError(err)
	if ok {
		ret := New(
			int32(FromGRPCCode(gs.Code())),
			gs.Message(),
		)
		for _, detail := range gs.Details() {
			switch d := detail.(type) {
			case *errdetails.ErrorInfo:
				ret.Reason = d.Reason
				return ret.WithMetadata(d.Metadata)
			}
		}
		return ret
	}
	return New(UnknownCode, err.Error())
}
