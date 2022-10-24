%%%-------------------------------------------------------------------
%%% @author akhil
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(helper).
-author("akhil").

%% API
-export([generateRandomNode/2,ahead_distance/4,get_nearest/5,nearest_node/3]).

generateRandomNode(Node, []) -> Node;
generateRandomNode(_, PrevNodes) -> lists:nth(rand:uniform(length(PrevNodes)), PrevNodes).

ahead_distance(K, K, _, Distance) ->
  Distance;

ahead_distance(K, Id, M, Distance) ->
  ahead_distance(K, (Id + 1) rem trunc(math:pow(2, M)), M, Distance + 1).

nearest_node(K, FingerId, State) ->
  case lists:member(K, FingerId) of
    true -> K;
    _ -> helper:get_nearest(K, FingerId, -1, 10000000, State)
  end.
get_nearest(_, [], _node, _, _) ->
  _node;
get_nearest(K, FingerId, _node, _min, State) ->
  [Finger| Others] = FingerId,

  Distance = helper:ahead_distance(K, Finger, dict:fetch(m, State), 0),
  if
    Distance < _min ->
      get_nearest(K, Others, Finger, Distance, State);
    true ->
      get_nearest(K, Others, _min, _min, State)
  end.
