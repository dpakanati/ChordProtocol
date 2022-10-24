%%%-------------------------------------------------------------------
%%% @author dhanush
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. Oct 2022 9:09 PM
%%%-------------------------------------------------------------------
-module(helper).
-author("dhanush").

%% API
-export([randomNode/2,get_forward_distance/4]).

randomNode(Node_id, []) -> Node_id;
randomNode(_, ExistingNodes) -> lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).

get_forward_distance(Key, Key, _, Distance) ->
  Distance;
get_forward_distance(Key, NodeId, M, Distance) ->
  get_forward_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1).