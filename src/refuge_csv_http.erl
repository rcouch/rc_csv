
% This file is part of refuge_csv released under the MIT license.
% See the LICENSE file for more information.

-module(refuge_csv_http).
-author("Nicolas R Dufour <nicolas.dufour@nemoworld.info>").

-export([handle_csv_req/2]).

-include_lib("couch/include/couch_db.hrl").

handle_csv_req(#httpd{method='POST'}=Req, Db) ->
    csv_req(Req, Db);
handle_csv_req(Req, _Db) ->
    couch_httpd:send_method_not_allowed(Req, "POST,HEAD").

%%

csv_req(Req, Db) ->
    ?LOG_DEBUG("csv was called with Req=~p and Db=~p!", [Req, Db]),

    do_csv_req(Req, Db).

do_csv_req(Req, Db) ->
    {ok, []}.

