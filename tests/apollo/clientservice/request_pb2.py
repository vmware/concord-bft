# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: request.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import any_pb2 as google_dot_protobuf_dot_any__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='request.proto',
  package='vmware.concord.client.request.v1',
  syntax='proto3',
  serialized_options=b'\n$com.vmware.concord.client.request.v1',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\rrequest.proto\x12 vmware.concord.client.request.v1\x1a\x1egoogle/protobuf/duration.proto\x1a\x19google/protobuf/any.proto\"\x92\x02\n\x07Request\x12\x15\n\x0braw_request\x18\x01 \x01(\x0cH\x00\x12-\n\rtyped_request\x18\x06 \x01(\x0b\x32\x14.google.protobuf.AnyH\x00\x12*\n\x07timeout\x18\x02 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x16\n\tread_only\x18\x03 \x01(\x08H\x01\x88\x01\x01\x12\x18\n\x0bpre_execute\x18\x04 \x01(\x08H\x02\x88\x01\x01\x12\x1b\n\x0e\x63orrelation_id\x18\x05 \x01(\tH\x03\x88\x01\x01\x42\x15\n\x13\x61pplication_requestB\x0c\n\n_read_onlyB\x0e\n\x0c_pre_executeB\x11\n\x0f_correlation_id\"j\n\x08Response\x12\x16\n\x0craw_response\x18\x01 \x01(\x0cH\x00\x12.\n\x0etyped_response\x18\x06 \x01(\x0b\x32\x14.google.protobuf.AnyH\x00\x42\x16\n\x14\x61pplication_response*\xe5\x02\n\x13\x43oncordErrorMessage\x12\x1d\n\x19\x43ONCORD_ERROR_UNSPECIFIED\x10\x00\x12!\n\x1d\x43ONCORD_ERROR_INVALID_REQUEST\x10\x01\x12\x1b\n\x17\x43ONCORD_ERROR_NOT_READY\x10\x02\x12\x19\n\x15\x43ONCORD_ERROR_TIMEOUT\x10\x03\x12%\n!CONCORD_ERROR_EXEC_DATA_TOO_LARGE\x10\x04\x12!\n\x1d\x43ONCORD_ERROR_EXEC_DATA_EMPTY\x10\x05\x12#\n\x1f\x43ONCORD_ERROR_CONFLICT_DETECTED\x10\x06\x12\x1c\n\x18\x43ONCORD_ERROR_OVERLOADED\x10\x07\x12+\n\'CONCORD_ERROR_EXECUTION_ENGINE_REJECTED\x10\x08\x12\x1a\n\x16\x43ONCORD_ERROR_INTERNAL\x10\t2o\n\x0eRequestService\x12]\n\x04Send\x12).vmware.concord.client.request.v1.Request\x1a*.vmware.concord.client.request.v1.ResponseB&\n$com.vmware.concord.client.request.v1b\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_duration__pb2.DESCRIPTOR,google_dot_protobuf_dot_any__pb2.DESCRIPTOR,])

_CONCORDERRORMESSAGE = _descriptor.EnumDescriptor(
  name='ConcordErrorMessage',
  full_name='vmware.concord.client.request.v1.ConcordErrorMessage',
  filename=None,
  file=DESCRIPTOR,
  create_key=_descriptor._internal_create_key,
  values=[
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_UNSPECIFIED', index=0, number=0,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_INVALID_REQUEST', index=1, number=1,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_NOT_READY', index=2, number=2,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_TIMEOUT', index=3, number=3,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_EXEC_DATA_TOO_LARGE', index=4, number=4,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_EXEC_DATA_EMPTY', index=5, number=5,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_CONFLICT_DETECTED', index=6, number=6,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_OVERLOADED', index=7, number=7,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_EXECUTION_ENGINE_REJECTED', index=8, number=8,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CONCORD_ERROR_INTERNAL', index=9, number=9,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=496,
  serialized_end=853,
)
_sym_db.RegisterEnumDescriptor(_CONCORDERRORMESSAGE)

ConcordErrorMessage = enum_type_wrapper.EnumTypeWrapper(_CONCORDERRORMESSAGE)
CONCORD_ERROR_UNSPECIFIED = 0
CONCORD_ERROR_INVALID_REQUEST = 1
CONCORD_ERROR_NOT_READY = 2
CONCORD_ERROR_TIMEOUT = 3
CONCORD_ERROR_EXEC_DATA_TOO_LARGE = 4
CONCORD_ERROR_EXEC_DATA_EMPTY = 5
CONCORD_ERROR_CONFLICT_DETECTED = 6
CONCORD_ERROR_OVERLOADED = 7
CONCORD_ERROR_EXECUTION_ENGINE_REJECTED = 8
CONCORD_ERROR_INTERNAL = 9



_REQUEST = _descriptor.Descriptor(
  name='Request',
  full_name='vmware.concord.client.request.v1.Request',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='raw_request', full_name='vmware.concord.client.request.v1.Request.raw_request', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='typed_request', full_name='vmware.concord.client.request.v1.Request.typed_request', index=1,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='timeout', full_name='vmware.concord.client.request.v1.Request.timeout', index=2,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='read_only', full_name='vmware.concord.client.request.v1.Request.read_only', index=3,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='pre_execute', full_name='vmware.concord.client.request.v1.Request.pre_execute', index=4,
      number=4, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='correlation_id', full_name='vmware.concord.client.request.v1.Request.correlation_id', index=5,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='application_request', full_name='vmware.concord.client.request.v1.Request.application_request',
      index=0, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_read_only', full_name='vmware.concord.client.request.v1.Request._read_only',
      index=1, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_pre_execute', full_name='vmware.concord.client.request.v1.Request._pre_execute',
      index=2, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
    _descriptor.OneofDescriptor(
      name='_correlation_id', full_name='vmware.concord.client.request.v1.Request._correlation_id',
      index=3, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
  ],
  serialized_start=111,
  serialized_end=385,
)


_RESPONSE = _descriptor.Descriptor(
  name='Response',
  full_name='vmware.concord.client.request.v1.Response',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='raw_response', full_name='vmware.concord.client.request.v1.Response.raw_response', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='typed_response', full_name='vmware.concord.client.request.v1.Response.typed_response', index=1,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
    _descriptor.OneofDescriptor(
      name='application_response', full_name='vmware.concord.client.request.v1.Response.application_response',
      index=0, containing_type=None,
      create_key=_descriptor._internal_create_key,
    fields=[]),
  ],
  serialized_start=387,
  serialized_end=493,
)

_REQUEST.fields_by_name['typed_request'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_REQUEST.fields_by_name['timeout'].message_type = google_dot_protobuf_dot_duration__pb2._DURATION
_REQUEST.oneofs_by_name['application_request'].fields.append(
  _REQUEST.fields_by_name['raw_request'])
_REQUEST.fields_by_name['raw_request'].containing_oneof = _REQUEST.oneofs_by_name['application_request']
_REQUEST.oneofs_by_name['application_request'].fields.append(
  _REQUEST.fields_by_name['typed_request'])
_REQUEST.fields_by_name['typed_request'].containing_oneof = _REQUEST.oneofs_by_name['application_request']
_REQUEST.oneofs_by_name['_read_only'].fields.append(
  _REQUEST.fields_by_name['read_only'])
_REQUEST.fields_by_name['read_only'].containing_oneof = _REQUEST.oneofs_by_name['_read_only']
_REQUEST.oneofs_by_name['_pre_execute'].fields.append(
  _REQUEST.fields_by_name['pre_execute'])
_REQUEST.fields_by_name['pre_execute'].containing_oneof = _REQUEST.oneofs_by_name['_pre_execute']
_REQUEST.oneofs_by_name['_correlation_id'].fields.append(
  _REQUEST.fields_by_name['correlation_id'])
_REQUEST.fields_by_name['correlation_id'].containing_oneof = _REQUEST.oneofs_by_name['_correlation_id']
_RESPONSE.fields_by_name['typed_response'].message_type = google_dot_protobuf_dot_any__pb2._ANY
_RESPONSE.oneofs_by_name['application_response'].fields.append(
  _RESPONSE.fields_by_name['raw_response'])
_RESPONSE.fields_by_name['raw_response'].containing_oneof = _RESPONSE.oneofs_by_name['application_response']
_RESPONSE.oneofs_by_name['application_response'].fields.append(
  _RESPONSE.fields_by_name['typed_response'])
_RESPONSE.fields_by_name['typed_response'].containing_oneof = _RESPONSE.oneofs_by_name['application_response']
DESCRIPTOR.message_types_by_name['Request'] = _REQUEST
DESCRIPTOR.message_types_by_name['Response'] = _RESPONSE
DESCRIPTOR.enum_types_by_name['ConcordErrorMessage'] = _CONCORDERRORMESSAGE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Request = _reflection.GeneratedProtocolMessageType('Request', (_message.Message,), {
  'DESCRIPTOR' : _REQUEST,
  '__module__' : 'request_pb2'
  # @@protoc_insertion_point(class_scope:vmware.concord.client.request.v1.Request)
  })
_sym_db.RegisterMessage(Request)

Response = _reflection.GeneratedProtocolMessageType('Response', (_message.Message,), {
  'DESCRIPTOR' : _RESPONSE,
  '__module__' : 'request_pb2'
  # @@protoc_insertion_point(class_scope:vmware.concord.client.request.v1.Response)
  })
_sym_db.RegisterMessage(Response)


DESCRIPTOR._options = None

_REQUESTSERVICE = _descriptor.ServiceDescriptor(
  name='RequestService',
  full_name='vmware.concord.client.request.v1.RequestService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=855,
  serialized_end=966,
  methods=[
  _descriptor.MethodDescriptor(
    name='Send',
    full_name='vmware.concord.client.request.v1.RequestService.Send',
    index=0,
    containing_service=None,
    input_type=_REQUEST,
    output_type=_RESPONSE,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_REQUESTSERVICE)

DESCRIPTOR.services_by_name['RequestService'] = _REQUESTSERVICE

# @@protoc_insertion_point(module_scope)
