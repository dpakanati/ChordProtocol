%%%-------------------------------------------------------------------
%%% @author dhanush,akhil
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2022 5:25 PM
%%%-------------------------------------------------------------------
-module(chord).
-author("dhanush,akhil").

-export([create_network/2, node/4, checkCompletion/2, main/2]).

main(_nodes, _requests) ->
  %io:fwrite("Start time: ~p",[erlang:localtime()]),
  {{_,_,_},{_,Min,Sec}} = erlang:localtime(),
  io:fwrite("Start time Min: ~p, Sec: ~p\n",[Min, Sec]),
  register(main, spawn(chord, create_network, [_nodes, _requests])).
create_network(_nodecount, _requests) ->
  M = trunc(math:ceil(math:log2(_nodecount))),
  [_Nodes, _State] = start_Nodes([], round(math:pow(2, M)), M, _nodecount, dict:new()),
  sendTables(_State,M),
  stabilize(_Nodes, _State),
  sendAndKill(_Nodes, _nodecount, _requests, M, _State).

start_Nodes(_Nodes, _, _, 0, _State) ->
  [_Nodes, _State];
start_Nodes(_Nodes, _Total, M, NumNodes, _State) ->
  [_hash, New_State] = addToNetwork(_Nodes, _Total,  M, _State),
  start_Nodes(lists:append(_Nodes, [_hash]), _Total, M, NumNodes - 1, New_State).
sendTables(_State,M) ->
  _fingerTabs = collectTable(_State, dict:to_list(_State), dict:new(),M),
  tabNodes(dict:fetch_keys(_fingerTabs), _State, _fingerTabs).
stabilize(_Nodes, _State) ->
  Pid = getID(lists:nth(rand:uniform(length(_Nodes)), _Nodes), _State),
  case Pid of
    stable -> stabilize(_Nodes, _State);
    _ -> Pid ! {stabilize, _State}
  end,
  io:fwrite("\n Wait...\n").
sendAndKill(_Nodes, NumNodes, _NumReqs, M, _State) ->
  register(completed, spawn(chord, checkCompletion, [NumNodes * _NumReqs, 0])),
  broadcast_message(_Nodes, _NumReqs, M, _State),
  TotalHops = totalHops(),

  io:format("~n Average Hops = ~p, Total Hops: ~p, Node Connections: ~p ~n", [(TotalHops/(NumNodes * _NumReqs)), TotalHops, NumNodes * _NumReqs]),
  {{_,_,_},{_,Min,Sec}} = erlang:localtime(),
  io:fwrite("End time Min: ~p, Sec: ~p.\n",[Min, Sec]),
  %io:fwrite("End time: ~p...\n",[erlang:localtime()]),
  kill(_Nodes, _State).

listen(NodeState) ->
  _hash = dict:fetch(id, NodeState),
  receive
    {finger_table, _fingerTab} ->
      UpdatedState = dict:store(finger_table, _fingerTab, NodeState);
    {lookup, Id, _Key, _Count, _} ->
      _NodeVal = helper:nearest_node(_Key, dict:fetch_keys(dict:fetch(finger_table ,NodeState)), NodeState),
      UpdatedState = NodeState,
      if
        (_hash == _Key) ->
          completed ! {completed, _hash, _Count, _Key};
        (_NodeVal == _Key) and (_hash =/= _Key) ->
          completed ! {completed, _hash, _Count, _Key};

        true ->
          dict:fetch(_NodeVal, dict:fetch(finger_table, NodeState)) ! {lookup, Id, _Key, _Count + 1, self()}
      end;
    {kill} ->
      UpdatedState = NodeState,
      exit("received exit signal");
    {state, Pid} -> Pid ! NodeState,
      UpdatedState = NodeState;
    {stabilize, _State} -> ok,
      UpdatedState = NodeState
  end,
  listen(UpdatedState).

node(_hash, M, _Nodes, NodeState) ->
  _fingerTab = lists:duplicate(M, helper:generateRandomNode(_hash, _Nodes)),
  NodeStateUpdated = dict:from_list([{id, _hash}, {predecessor, stable}, {finger_table, _fingerTab}, {next, 0}, {m, M}]),
  listen(NodeStateUpdated).

getID(_hash, _State) ->
  case dict:find(_hash, _State) of
    error -> stable;
    _ -> dict:fetch(_hash, _State)
  end.

addToNetwork(_Nodes, _Total, M, _State) ->
  _OtherHashes = lists:seq(0, _Total - 1, 1) -- _Nodes,
  _hash = lists:nth(rand:uniform(length(_OtherHashes)), _OtherHashes),
  Pid = spawn(chord, node, [_hash, M, _Nodes, dict:new()]),
  [_hash, dict:store(_hash, Pid, _State)].


checkCompletion(0, _Count) ->
  main ! {totalhops, _Count};

checkCompletion(_NumReqss, _Count) ->
  receive
    {completed, _, _CountForTask, _} ->
      checkCompletion(_NumReqss - 1, _Count + _CountForTask)
  end.

to_node(_, [], _) ->
  ok;
to_node(_Key, _Nodes, _State) ->
  [First | Other] = _Nodes,
  Pid = getID(First, _State),
  Pid ! {lookup, First, _Key, 0, self()},
  to_node(_Key, Other, _State).

broadcast_message(_, 0, _, _) ->
  ok;
broadcast_message(_Nodes, _NumReqs, M, _State) ->
  timer:sleep(1000),
  _Key = lists:nth(rand:uniform(length(_Nodes)), _Nodes),
  to_node(_Key, _Nodes, _State),
  broadcast_message(_Nodes, _NumReqs - 1, M, _State).

kill([], _) ->
  ok;
kill(_Nodes, _State) ->
  [_First | Other] = _Nodes,
  getID(_First, _State) ! {kill},
  kill(Other, _State).

totalHops() ->
  receive
    {totalhops, _Count} ->
      _Count
  end.

n_successor(_, _, I , I, _CurID, _) ->
  _CurID;

n_successor(_hash, _State, I, _Cur, _CurID, M) ->
  case dict:find((_CurID + 1) rem trunc(math:pow(2, M)), _State) of
    error ->
      n_successor(_hash, _State, I, _Cur, (_CurID + 1) rem trunc(math:pow(2, M)),M);
    _ -> n_successor(_hash, _State, I, _Cur + 1, (_CurID + 1) rem trunc(math:pow(2, M)),M)
  end.

getTable(_, _, M, M,_List) ->
  _List;
getTable(_Node, _State, M, I, _List) ->
  _hash = element(1, _Node),
  Ith_succesor = n_successor(_hash, _State, trunc(math:pow(2, I)), 0, _hash, M),
  getTable(_Node, _State, M, I + 1, _List ++ [{Ith_succesor, dict:fetch(Ith_succesor, _State)}] ).

collectTable(_, [], _FTDict,_) ->
  _FTDict;

collectTable(_State, _NetList, _FTDict,M) ->
  [_First | Other] = _NetList,
  _fingerTabs = getTable(_First, _State,M, 0,[]),
  collectTable(_State, Other, dict:store(element(1, _First), _fingerTabs, _FTDict), M).

tabNodes([], _, _) ->
  ok;
tabNodes(_Nodes, _State, _fingerTabs) ->
  [First|Other] = _Nodes,
  Pid = dict:fetch(First ,_State),
  Pid ! {finger_table, dict:from_list(dict:fetch(First, _fingerTabs))},
  tabNodes(Other, _State, _fingerTabs).