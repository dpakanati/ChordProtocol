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
-export([randomNode/2,get_forward_distance/4,get_closest/5,get_closest_node/3]).

randomNode(Node_id, []) -> Node_id;
randomNode(_, ExistingNodes) -> lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).

get_forward_distance(Key, Key, _, Distance) ->
  Distance;
get_forward_distance(Key, NodeId, M, Distance) ->
  get_forward_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1).
get_closest_node(Key, FingerNodeIds, State) ->
  case lists:member(Key, FingerNodeIds) of
    true -> Key;
    _ -> helper:get_closest(Key, FingerNodeIds, -1, 10000000, State)
  end.
get_closest(_, [], MinNode, _, _) ->
  MinNode;
get_closest(Key, FingerNodeIds, MinNode, MinVal, State) ->
  [First| Rest] = FingerNodeIds,

  Distance = helper:get_forward_distance(Key, First, dict:fetch(m, State), 0),
  if
    Distance < MinVal ->
      get_closest(Key, Rest, First, Distance, State);
    true ->
      get_closest(Key, Rest, MinNode, MinVal, State)
  end.
