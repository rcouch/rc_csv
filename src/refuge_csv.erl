
% This file is part of refuge_csv released under the MIT license.
% See the LICENSE file for more information.

-module(refuge_csv).
-author("Nicolas R Dufour <nicolas.dufour@nemoworld.info>").

-include_lib("couch/include/couch_db.hrl").
-include_lib("refuge_csv/include/refuge_csv.hrl").

-export([
    import_csv_in_db/2
]).

import_csv_in_db(Db, Args) ->

    RowFunction = fun
        ({eof}, State) ->
            ?LOG_DEBUG("The stread has ended!", []),
            State;
        ({newline, NewLine}, State) ->
            ?LOG_DEBUG("I got a newLine: ~p", [NewLine]),
            State
    end,

    StreamProcessingFun = fun
        ({eof}, State) ->
            ecsv_parser:end_parsing(State);
        ({next, Chunk}, State) ->
            String = binary_to_list(Chunk),
            lists:foldl(
                fun(C, Acc) ->
                    ecsv_parser:parse_with_character(clean_char_argument(C), Acc)
                end,
                State,
                String
            )
    end,

    InitState = ecsv_parser:init(RowFunction, []),

    StreamingFun = Args#rcargs.streaming_fun,
    StreamingFun(StreamProcessingFun, InitState),

    ok.

%% @doc make sure that an integer denoting a char is returned instead of a string
clean_char_argument([CharInt | _]) ->
    CharInt;
clean_char_argument(CharInt) when is_integer(CharInt) ->
    CharInt.
