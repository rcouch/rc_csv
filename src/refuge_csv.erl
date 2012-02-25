
% This file is part of refuge_csv released under the MIT license.
% See the LICENSE file for more information.

-module(refuge_csv).
-author("Nicolas R Dufour <nicolas.dufour@nemoworld.info>").

-include_lib("couch/include/couch_db.hrl").
-include_lib("refuge_csv/include/refuge_csv.hrl").

-export([
    import_csv_in_db/2, loop/2
]).

import_csv_in_db(Db, Args) ->

    RowFunction = fun(NewLine, State) ->
        ?LOG_DEBUG("I got a newLine: ~p", [NewLine]),
        State
    end,

    ProcessingPid = spawn(?MODULE, loop, [RowFunction, []]),
    ParsingPid = spawn(ecsv_parser, start_parsing, [ProcessingPid]),

    StreamProcessingFun = fun
        ({eof}, Acc) ->
%?LOG_DEBUG("The stream has ended", []),
            ParsingPid ! {eof},
            Acc;
        ({next, Chunk}, Acc) ->
%?LOG_DEBUG("Got a new chunk: ~p", [Chunk]),
            F = fun(X) ->
                ParsingPid ! {char, X},
%?LOG_DEBUG("Sending a character ~p", [X]),
                X
            end,
            << <<(F(X))>> || <<X>> <= Chunk >>,
            Acc
    end,

    StreamingFun = Args#rcargs.streaming_fun,
    spawn(fun() -> StreamingFun(StreamProcessingFun, []) end),

    ok.

loop(RowFunction, State) ->
    receive
        % ignore empty row
        {newline, [[]]} ->
            loop(RowFunction, State);
        % process a new row
        {newline, NewLine} ->
            NewState = RowFunction(NewLine, State),
            loop(RowFunction, NewState);
        % the parsing is done, time to stop
        {done} ->
            {ok, State}
    end.

