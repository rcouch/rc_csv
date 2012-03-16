
% This file is part of refuge_csv released under the MIT license.
% See the LICENSE file for more information.

-module(refuge_csv).
-author("Nicolas R Dufour <nicolas.dufour@nemoworld.info>").

-include_lib("couch/include/couch_db.hrl").
-include_lib("refuge_csv/include/refuge_csv.hrl").

-export([
    import_csv_in_db/2
]).

-record(pstate, {
    db,
    docs = [],
    headers = []
}).

import_csv_in_db(Db, Args) ->
    InitProcessingState = #pstate{db=Db},

    InitState = ecsv_parser:init(fun process_csv_row/2, InitProcessingState),

    StreamingFun = Args#rcargs.streaming_fun,
    StreamingFun(fun process_chunk/2, InitState),

    ok.

% -----------------------------------------------------------------------------

-define(BULK_SIZE, 500).

process_csv_row({eof}, State) ->
    State;
process_csv_row({newline, NewLine}, #pstate{headers=Headers, docs=Docs, db=Db}=State) ->
    case Headers of
        [] ->
            State#pstate{headers=NewLine};
        _  ->
            % create a document from the header and the new row
            {ok, Doc} = make_doc(Headers, NewLine),
            Docs1 = [Doc|Docs],

            if
                length(Docs1) >= ?BULK_SIZE ->
                    {ok, _Results} = couch_db:update_docs(Db, Docs1, []),
                    State#pstate{docs=[]};
                true ->
                    State#pstate{docs=Docs1}
            end
    end.

make_doc(Headers, Row) ->
    Props = lists:zipwith(
        fun(K, V) ->
            {iolist_to_binary(K), iolist_to_binary(V)}
        end,
        Headers,
        Row
    ),

    IdProp = {<<"_id">>, couch_uuids:new()},
    PropsWithId = [IdProp|Props],

    {ok, couch_doc:from_json_obj({PropsWithId})}.

% -----------------------------------------------------------------------------

process_chunk({eof}, State) ->
    ecsv_parser:end_parsing(State);
process_chunk({next, Chunk}, State) ->
    String = binary_to_list(Chunk),
    lists:foldl(
        fun(C, Acc) ->
            ecsv_parser:parse_with_character(clean_char_argument(C), Acc)
        end,
        State,
        String
    ).

%% @doc make sure that an integer denoting a char is returned instead of a string
clean_char_argument([CharInt | _]) ->
    CharInt;
clean_char_argument(CharInt) when is_integer(CharInt) ->
    CharInt.
