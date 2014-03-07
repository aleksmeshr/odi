%%% Copyright (C) 2013 - Aleksandr Mescheryakov.  All rights reserved.

-module(odi_bin).

-export([encode/2, decode/2,
         null/0,
         decode_tuple/2,
         decode_tuples/3,
         decode_error/1,
         decode_record/1,
         encode_record_type/1,
         decode_record_type/1,
         encode_record_vc/1,
         decode_linked_record/1,
         decode_record_list/3,
         switch/3]).

-include("../include/odi.hrl").

encode({T, V}) -> encode(T, V).
encode(_Any, null)                  -> <<-1:?o_int>>;
encode(bool, true)                  -> <<1:?o_byte>>;
encode(bool, false)                 -> <<0:?o_byte>>;
encode(byte, N)                     -> <<N:?o_byte>>;
encode(short, N)                    -> <<N:?o_short>>;
encode(integer, N)                  -> <<N:?o_int>>;
encode(long, N)                     -> <<N:?o_long>>;
encode(float, N)                    -> <<N:?o_float>>;
encode(double, N)                   -> <<N:?o_double>>;
encode(rawbytes, B)                 -> B;
encode(bytes, S) -> encode(string, S);
encode(string, B) when is_binary(B) -> <<(byte_size(B)):?o_int, B/binary>>;
encode(string, B) when is_list(B) -> Bin = iolist_to_binary(B), <<(byte_size(Bin)):?o_int, Bin/binary>>;
encode(strings, L) when is_list(L) 	-> <<(length(L)):?o_int,
												(iolist_to_binary([encode(string, S) || S <- L]))/binary>>;
encode(TypeList, ValueList) when is_list(TypeList) ->
	iolist_to_binary([encode(T_V) || T_V <- lists:zip(TypeList, ValueList)]);
encode({zero_end, TypeList}, ValueLists) ->
  iolist_to_binary([[encode(TypeList, V) || V <- ValueLists], <<0:?o_byte>>]);
encode({CountType, TypeList}, ValueLists) ->
  iolist_to_binary([encode(CountType, length(ValueLists)), [encode(TypeList, V) || V <- ValueLists]]);
encode(Type, Value)                       -> {encode_error, Type, Value}.

null() -> <<-1:?o_int>>.

decode_strings(Rest, Strings, 0) ->
    {lists:reverse(Strings), Rest};
decode_strings(<<Len:?o_int, Rest/binary>>, Strings, Count) ->
    decode_strings(binary:part(Rest, Len, byte_size(Rest)-Len), [binary_to_list(binary:part(Rest, 0, Len)) | Strings], Count-1).

decode(bool, <<1:?o_byte, Rest/binary>>)     -> {true, Rest};
decode(bool, <<0:?o_byte, Rest/binary>>)     -> {false, Rest};
decode(byte, <<N:?o_byte, Rest/binary>>)    -> {N, Rest};
decode(short, <<N:?o_short, Rest/binary>>)    -> {N, Rest};
decode(integer, <<N:?o_int, Rest/binary>>)    -> {N, Rest};
decode(long, <<N:?o_long, Rest/binary>>)    -> {N, Rest};
decode(float, <<N:?o_float, Rest/binary>>)   -> {N, Rest};
decode(double, <<N:?o_double, Rest/binary>>)   -> {N, Rest};
decode(bytes, <<-1:?o_int, Rest/binary>>) ->
    {null, Rest};
decode(bytes, <<Len:?o_int, Rest/binary>>) ->
    {binary:part(Rest, 0, Len), binary:part(Rest, Len, byte_size(Rest)-Len)};
decode(string, <<-1:?o_int, Rest/binary>>) ->
    {null, Rest};
decode(string, Bin) ->
    decode(bytes, Bin);
    %{binary_to_list(binary:part(Rest, 0, Len)), binary:part(Rest, Len, byte_size(Rest)-Len)};
decode(strings, <<Count:?o_int, Rest/binary>>) ->
    decode_strings(Rest, [], Count);
decode(TypeList, Bin) when is_list(TypeList) ->
	decode_tuple(TypeList, Bin);
decode({zero_end, TypeList}, Bin) ->
  decode_tuples_zero(TypeList, Bin);
decode({CountType, TypeList}, Bin) ->
	decode_tuples(CountType, TypeList, Bin);
decode(Type, Bin)                         -> {{decode_error, Type, Bin}}.

decode_tuple(TypeList, Bin) ->
	decode_tuple(TypeList, Bin, []).
decode_tuple([], Bin, ValuesAcc) ->
	{list_to_tuple(lists:reverse(ValuesAcc)), Bin};
decode_tuple(TypeList, Bin, ValuesAcc) ->
	[Type|RestTypes] = TypeList,
  case decode(Type, Bin) of
	    {Value, RestBin} -> decode_tuple(RestTypes, RestBin, [Value|ValuesAcc]);
      Error -> Error
  end.

% decode data in format: Len:Type0,[Field1:Type1,...]
decode_tuples(CountType, TypeList, Bin) ->
  case decode(CountType, Bin) of
  	{Count, Rest} -> decode_tuples(Count, TypeList, Rest, []);
    Error -> Error
  end.
decode_tuples(0, _TypeList, Bin, ValuesAcc) ->
	{{length(ValuesAcc), lists:reverse(ValuesAcc)}, Bin};
decode_tuples(Count, _TypeList, Bin, _ValuesAcc) when Count<0 ->
	{[], Bin};
decode_tuples(Count, TypeList, Bin, ValuesAcc) ->
  case decode_tuple(TypeList, Bin) of
      {Values, Rest} -> decode_tuples(Count-1, TypeList, Rest, [Values | ValuesAcc]);
      Error -> Error
  end.

decode_tuples_zero(TypeList, Bin) ->
  decode_tuples_zero(TypeList, Bin, []).
decode_tuples_zero(_TypeList, <<0:?o_byte, Rest/binary>>, ValuesAcc) ->
  {{length(ValuesAcc), lists:reverse(ValuesAcc)}, Rest};
decode_tuples_zero(TypeList, Bin, ValuesAcc) ->
  case decode_tuple(TypeList, Bin) of
    {Values, Rest} -> decode_tuples_zero(TypeList, Rest, [Values | ValuesAcc]);
    Error -> Error
  end.

encode_record_type(RecordType) ->
    {_, Code} = lists:keyfind(RecordType, 1, [{raw, $b}, {flat, $f}, {document, $d}]),
    Code.

decode_record_type(RecordTypeByte) ->
    {Type, _} = lists:keyfind(RecordTypeByte, 2, [{raw, $b}, {flat, $f}, {document, $d}]),
    Type.

encode_record_vc(RecordVersionControl) ->
    {_, Code} = lists:keyfind(RecordVersionControl, 1, [{vc, 0}, {inc_no_vc, -1}, {no_vc, -2}]),
    Code.

%Decode error response from OrientDB
%The format is: [(1)(exception-class:string)(exception-message:string)]*(0)
decode_error(Bin) ->
  decode_error(Bin, []).
decode_error(<<0:?o_byte, Rest/binary>>, ErrorsAcc) ->
  {lists:reverse(ErrorsAcc), Rest};
decode_error(<<1:?o_byte, ErrorInfo/binary>>, ErrorsAcc) ->
  case decode([string, string], ErrorInfo) of
    {{ExceptionClass, ExceptionMessage}, Rest} -> decode_error(Rest, [{ExceptionClass, ExceptionMessage} | ErrorsAcc]);
    Error -> Error
  end;
decode_error(<<Data/binary>>, _) ->
  {{error_decoding_errinfo, Data}}.

decode_record(Bin) ->
  {{RecordContent, RecordVersion, RecordTypeCode}, Rest}
      = decode([bytes, integer, byte], Bin),
  RecordType = decode_record_type(RecordTypeCode),
  Result = decode_record_content(RecordContent, RecordType),
  {LinkedRecords, Rest2} = decode_linked_records(Rest, []),
  {{RecordType, RecordVersion, Result, LinkedRecords}, Rest2}.

decode_record_content(Bin, document)->
  try odi_doc:decode(Bin) of
    Ret -> Ret
  catch
    E:E1 ->
      {ErrorName, Contex} = E1,
      {E,ErrorName, binary_to_list(Contex)}
  end;
decode_record_content(Bin, raw) ->
  Bin;
decode_record_content(Bin, flat) ->
  Bin.

decode_record_list(Bin, 0, Acc) ->
  {lists:reverse(Acc), Bin};
decode_record_list(Bin, Count, Acc) ->
  {Record, Rest} = decode_linked_record(Bin),
  decode_record_list(Rest, Count-1, [Record | Acc]).

% Decode linked records
% 0 meens no more records
decode_linked_records(<<0:?o_byte, Rest/binary>>, Acc) ->
  {lists:reverse(Acc), Rest};
% 2 meens some record
decode_linked_records(<<2:?o_byte, Rest/binary>>, Acc) ->
  {Record, Rest2} = decode_linked_record(Rest),
  decode_linked_records(Rest2, [Record | Acc]).

% Decode RID of record
decode_linked_record(<<-3:?o_short, Data/binary>>) ->
  <<ClusterId:?o_short, ClusterPosition:?o_long, Rest/binary>> = Data,
  {{ClusterId, ClusterPosition}, Rest};
% No record
decode_linked_record(<<-2:?o_short, Rest/binary>>) ->
  {null, Rest};
% Record has no class-id
decode_linked_record(<<-1:?o_short, Rest/binary>>) ->
  {no_class_id, Rest};
% Valid record
decode_linked_record(<<0:?o_short, Data/binary>>) ->
  {{RecordTypeCode, ClusterId, ClusterPosition, RecordVersion, Record}, Rest}
      = decode([byte, short, long, integer, bytes], Data),
  RecordType = decode_record_type(RecordTypeCode),
  {{{ClusterId, ClusterPosition}, RecordType, RecordVersion, decode_record_content(Record, RecordType)}, Rest};
% Unknown status
decode_linked_record(<<Status:?o_short, Data/binary>>) ->
  {{record_decoding_error, Status, Data}}; %corrupt data - generates exception
decode_linked_record(<<Data/binary>>) ->
  {{record_decoding_error, Data}}. %incomplete or corrupt data - generates exception

switch(true, TrueExpr, _FalseExpr) ->
    TrueExpr;
switch(_, _TrueExpr, FalseExpr) ->
    FalseExpr.