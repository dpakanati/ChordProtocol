%%%-------------------------------------------------------------------
%%% @author dhanush
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2022 5:25 PM
%%%-------------------------------------------------------------------
-module(chord).
-author("dhanush").

-export([create_network/2, node/4, listen_task_completion/2, main/2]).

main(_nodes, _requests) ->

  io:fwrite("Start time: ~p",[erlang:localtime()]),
  register(main, spawn(chord, create_network, [_nodes, _requests])).
create_network(_nodecount, _requests) ->
  M = trunc(math:ceil(math:log2(_nodecount))),
  [_Nodes, _State] = create_nodes([], round(math:pow(2, M)), M, _nodecount, dict:new()),
  send_finger_tables(_State,M),
  stabilize(_Nodes, _State),
  send_messages_and_kill(_Nodes, _nodecount, _requests, M, _State).

create_nodes(_Nodes, _, _, 0, _State) ->
  [_Nodes, _State];
create_nodes(_Nodes, TotalNodes, M, NumNodes, _State) ->
  [Hash, New_State] = add_node_to_chord(_Nodes, TotalNodes,  M, _State),
  create_nodes(lists:append(_Nodes, [Hash]), TotalNodes, M, NumNodes - 1, New_State).
send_finger_tables(_State,M) ->
  FingerTables = collectfingertables(_State, dict:to_list(_State), dict:new(),M),
  %io:format("~n~p~n", [FingerTables]),
  send_finger_tables_nodes(dict:fetch_keys(FingerTables), _State, FingerTables).
stabilize(_Nodes, _State) ->
  Pid = get_node_pid(lists:nth(rand:uniform(length(_Nodes)), _Nodes), _State),
  case Pid of
    stable -> stabilize(_Nodes, _State);
    _ -> Pid ! {stabilize, _State}
  end,
  io:fwrite("\n Wait...\n").
send_messages_and_kill(_Nodes, NumNodes, NumRequest, M, _State) ->
  register(taskcompletionmonitor, spawn(chord, listen_task_completion, [NumNodes * NumRequest, 0])),

  send_messages_all_nodes(_Nodes, NumRequest, M, _State),

  TotalHops = getTotalHops(),

  io:format("~n Average Hops = ~p, Total Hops: ~p, Node Connections: ~p ~n", [TotalHops/(NumNodes * NumRequest), TotalHops, NumNodes * NumRequest]),
  io:fwrite("End time: ~p",[erlang:localtime()]),
  kill_all_nodes(_Nodes, _State).



randomNode(Node_id, []) -> Node_id;
randomNode(_, ExistingNodes) -> lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).


get_forward_distance(Key, Key, _, Distance) ->
  Distance;
get_forward_distance(Key, NodeId, M, Distance) ->
  get_forward_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1)
.

get_closest(_, [], MinNode, _, _) ->
  MinNode;
get_closest(Key, FingerNodeIds, MinNode, MinVal, State) ->
  [First| Rest] = FingerNodeIds,
  Distance = get_forward_distance(Key, First, dict:fetch(m, State), 0),
  if
    Distance < MinVal ->
      get_closest(Key, Rest, First, Distance, State);
    true ->
      get_closest(Key, Rest, MinNode, MinVal, State)
  end
.

get_closest_node(Key, FingerNodeIds, State) ->
  case lists:member(Key, FingerNodeIds) of
    true -> Key;
    _ -> get_closest(Key, FingerNodeIds, -1, 10000000, State)
  end

.

node_listen(NodeState) ->
  Hash = dict:fetch(id, NodeState),
  receive
    {finger_table, FingerTable} ->
      % io:format("Received Finger for ~p ~p", [Hash, FingerTable]),
      UpdatedState = dict:store(finger_table, FingerTable, NodeState);

    {lookup, Id, Key, HopsCount, Pid} ->

      NodeVal = get_closest_node(Key, dict:fetch_keys(dict:fetch(finger_table ,NodeState)), NodeState),
      UpdatedState = NodeState,
      %io:format("Lookup::: ~p  For Key ~p  ClosestNode ~p ~n", [Hash, Key, NodeVal]),
      if

        (Hash == Key) ->
          taskcompletionmonitor ! {completed, Hash, HopsCount, Key};
        (NodeVal == Key) and (Hash =/= Key) ->
          taskcompletionmonitor ! {completed, Hash, HopsCount, Key};

        true ->
          dict:fetch(NodeVal, dict:fetch(finger_table, NodeState)) ! {lookup, Id, Key, HopsCount + 1, self()}
      end
  ;
    {kill} ->
      UpdatedState = NodeState,
      exit("received exit signal");
    {state, Pid} -> Pid ! NodeState,
      UpdatedState = NodeState;
    {stabilize, _State} -> ok,
      %io:fwrite("Stabilzing the network"),
      UpdatedState = NodeState
  end,
  node_listen(UpdatedState).

node(Hash, M, _Nodes, NodeState) ->
  %io:format("Node is spawned with hash ~p",[Hash]),
  FingerTable = lists:duplicate(M, randomNode(Hash, _Nodes)),
  NodeStateUpdated = dict:from_list([{id, Hash}, {predecessor, stable}, {finger_table, FingerTable}, {next, 0}, {m, M}]),
  node_listen(NodeStateUpdated).


get_m(NumNodes) ->
  trunc(math:ceil(math:log2(NumNodes)))
.

get_node_pid(Hash, _State) ->
  case dict:find(Hash, _State) of
    error -> stable;
    _ -> dict:fetch(Hash, _State)
  end.

add_node_to_chord(_Nodes, TotalNodes, M, _State) ->
  RemainingHashes = lists:seq(0, TotalNodes - 1, 1) -- _Nodes,
  Hash = lists:nth(rand:uniform(length(RemainingHashes)), RemainingHashes),
  Pid = spawn(chord, node, [Hash, M, _Nodes, dict:new()]),
  %%:format("~n ~p ~p ~n", [Hash, Pid]),
  [Hash, dict:store(Hash, Pid, _State)].


listen_task_completion(0, HopsCount) ->
  main ! {totalhops, HopsCount};

listen_task_completion(NumRequests, HopsCount) ->
  receive
    {completed, Pid, HopsCountForTask, Key} ->
      % io:format("received completion from ~p, Number of Hops ~p, For Key ~p", [Pid, HopsCountForTask, Key]),
      listen_task_completion(NumRequests - 1, HopsCount + HopsCountForTask)
  end.

send_message_to_node(_, [], _) ->
  ok;
send_message_to_node(Key, _Nodes, _State) ->
  [First | Rest] = _Nodes,
  Pid = get_node_pid(First, _State),
  Pid ! {lookup, First, Key, 0, self()},
  send_message_to_node(Key, Rest, _State).


send_messages_all_nodes(_, 0, _, _) ->
  ok;
send_messages_all_nodes(_Nodes, NumRequest, M, _State) ->
  timer:sleep(1000),
  Key = lists:nth(rand:uniform(length(_Nodes)), _Nodes),
  send_message_to_node(Key, _Nodes, _State),
  send_messages_all_nodes(_Nodes, NumRequest - 1, M, _State)
.

kill_all_nodes([], _) ->
  ok;
kill_all_nodes(_Nodes, _State) ->
  [First | Rest] = _Nodes,
  get_node_pid(First, _State) ! {kill},
  kill_all_nodes(Rest, _State).

getTotalHops() ->
  receive
    {totalhops, HopsCount} ->
      HopsCount
  end.








get_ith_successor(_, _, I , I, CurID, M) ->
  CurID;

get_ith_successor(Hash, _State, I, Cur, CurID, M) ->
  case dict:find((CurID + 1) rem trunc(math:pow(2, M)), _State) of
    error ->
      get_ith_successor(Hash, _State, I, Cur, (CurID + 1) rem trunc(math:pow(2, M)),M);
    _ -> get_ith_successor(Hash, _State, I, Cur + 1, (CurID + 1) rem trunc(math:pow(2, M)),M)
  end
.

get_finger_table(_, _, M, M,FingerList) ->
  FingerList;
get_finger_table(Node, _State, M, I, FingerList) ->
  Hash = element(1, Node),
  Ith_succesor = get_ith_successor(Hash, _State, trunc(math:pow(2, I)), 0, Hash, M),
  get_finger_table(Node, _State, M, I + 1, FingerList ++ [{Ith_succesor, dict:fetch(Ith_succesor, _State)}] )
.


collectfingertables(_, [], FTDict,_) ->
  FTDict;

collectfingertables(_State, NetList, FTDict,M) ->
  [First | Rest] = NetList,
  FingerTables = get_finger_table(First, _State,M, 0,[]),
  collectfingertables(_State, Rest, dict:store(element(1, First), FingerTables, FTDict), M)
.

send_finger_tables_nodes([], _, _) ->
  ok;
send_finger_tables_nodes(NodesToSend, _State, FingerTables) ->
  [First|Rest] = NodesToSend,
  Pid = dict:fetch(First ,_State),
  Pid ! {finger_table, dict:from_list(dict:fetch(First, FingerTables))},
  send_finger_tables_nodes(Rest, _State, FingerTables)
.





