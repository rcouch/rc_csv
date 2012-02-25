
% This file is part of refuge_csv released under the MIT license.
% See the LICENSE file for more information.

-module(refuge_csv_http).
-author("Nicolas R Dufour <nicolas.dufour@nemoworld.info>").

-export([
    handle_csv_req/2,
    parse_qs/1
]).

-include_lib("couch/include/couch_db.hrl").
-include_lib("refuge_csv/include/refuge_csv.hrl").

%%

handle_csv_req(#httpd{method='POST'}=Req, Db) ->
    csv_req(Req, Db);
handle_csv_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "POST,HEAD").

parse_qs(Req) ->
    Args = #rcargs{ },
    lists:foldl(fun({K, V}, Acc) ->
        parse_qs(K, V, Acc)
    end, Args, couch_httpd:qs(Req)).

%%

%csv_req(Req, Db) ->
%    Files = couch_httpd:parse_form(Req),
%    ?LOG_DEBUG("Files: ~p", [Files]),
%    couch_httpd:send_json(Req, {[ {foo, bar} ]}).

csv_req(Req, Db) ->
    % TODO need to add authorization stuff here
    do_csv_req(Req, Db).

do_csv_req(Req, Db) ->
    Args0 = parse_qs(Req),
    
    StreamingFun = streaming_fun(Req),
    Args1 = Args0#rcargs{streaming_fun=StreamingFun},

    case refuge_csv:import_csv_in_db(Db, Args1) of
        ok ->
            couch_httpd:send_json(Req, {[
                {tranform, Args1#rcargs.transform},
                {delimiter, list_to_binary([Args1#rcargs.delimiter])}
            ]});
        {error, Error} ->
            throw({error, Error});
        Unknown ->
            throw({error, Unknown})
    end.

parse_qs(Key, Val, Args) ->
    case Key of
        "" ->
            Args;
        "transform" ->
            Args#rcargs{transform=list_to_binary(Val)};
        "delimiter" ->
            Args#rcargs{delimiter=parse_char(Val)};
        _  ->
            Args
    end.

parse_char(Val) ->
    Binary = list_to_binary(Val),
    case Binary of
        <<D:8>> ->
            D;
        _ ->
            throw({query_parse_error, <<"Wrong delimiter format. Must be a single character">>})
    end.

streaming_fun(Req) ->
    Fun = fun(Fun, InitState) ->
        ChunkFun =
            fun
                ({0, _Footers}, Acc) ->
                    Fun({eof}, Acc);
                ({_Len, Chunk}, Acc) ->
                    Fun({next, Chunk}, Acc)
            end,

        couch_httpd:recv_chunked(Req, 8192, ChunkFun, InitState)
    end,

    Fun.

%% The End
    
