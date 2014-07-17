-module(goodride_db_utils).

-export([cursor_to_records/2]).

dec2hex(Dec) ->
    dec2hex(<<>>, Dec).

dec2hex(N, <<I:8,Rem/binary>>) ->
    dec2hex(<<N/binary, (hex0((I band 16#f0) bsr 4)):8, (hex0((I band 16#0f))):8>>, Rem);
dec2hex(N,<<>>) ->
    N.

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I) ->  $0 + I.

is_id_attr(AttrName) ->
    lists:suffix("_id", atom_to_list(AttrName)).

unpack_id(_Type, undefined) ->
    undefined;
unpack_id(Type, MongoId) ->
    lists:concat([Type, "-", binary_to_list(dec2hex(element(1, MongoId)))]).

unpack_value(_AttrName, [H|T], _ValueType) when is_integer(H) ->
    {integers, [H|T]};
unpack_value(_AttrName, {_, _, _} = Value, datetime) ->
    calendar:now_to_datetime(Value);
unpack_value(AttrName, Value, ValueType) ->
    case is_id_attr(AttrName) and (Value =/= "") of
        true ->
            IdType = id_type_from_foreign_key(AttrName),
            unpack_id(IdType, Value);
        false ->
            boss_record_lib:convert_value_to_type(Value, ValueType)
    end.

id_type_from_foreign_key(ForeignKey) ->
    Tokens = string:tokens(atom_to_list(ForeignKey), "_"),
    NameTokens = lists:filter(fun(Token) -> Token =/= "id" end,
        Tokens),
    string:join(NameTokens, "_").


% Boss and MongoDB have a different conventions to id attributes (id vs. '_id').
attr_value(id, MongoDoc) ->
    proplists:get_value('_id', MongoDoc);
attr_value(AttrName, MongoDoc) ->
    proplists:get_value(AttrName, MongoDoc).


% The mongodb driver uses "associative tuples" which look like:
%      {key1, Value1, key2, Value2}
tuple_to_proplist(Tuple) ->
    List = tuple_to_list(Tuple),
    Ret = lists:reverse(list_to_proplist(List, [])),
    Ret.

list_to_proplist([], Acc) -> Acc;
list_to_proplist([K,V|T], Acc) ->
    list_to_proplist(T, [{K, V}|Acc]).

mongo_make_args(Type, MongoDoc, AttributeTypes, AttributeNames) ->
    lists:map(fun
		  (id) ->
		      MongoValue = attr_value(id, MongoDoc),
		      unpack_id(Type, MongoValue);
		  (AttrName) ->
		      MongoValue = attr_value(AttrName, MongoDoc),
		      ValueType = proplists:get_value(AttrName, AttributeTypes),
		      unpack_value(AttrName, MongoValue, ValueType)
	      end,
              AttributeNames).

% Convert a tuple return by the MongoDB driver to a Boss record
mongo_tuple_to_record(Type, Row) ->
    MongoDoc		= tuple_to_proplist(Row),
    AttributeTypes	= boss_record_lib:attribute_types(Type),
    AttributeNames	= boss_record_lib:attribute_names(Type),
    Args                = mongo_make_args(Type, MongoDoc, AttributeTypes,
					  AttributeNames),
    apply(Type, new, Args).

cursor_to_records(Type, Curs) ->
  lists:map(fun(Row) ->
                mongo_tuple_to_record(Type, Row)
            end,
            mongo:rest(Curs)).
