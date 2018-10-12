%%--------------------------------------------------------------------
%% Copyright (c) 2015-2017 Feng Lee <feng@emqtt.io>.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emq_bridge_kafka).

-include_lib("emqttd/include/emqttd.hrl").

-include_lib("emqttd/include/emqttd_protocol.hrl").

-include_lib("emqttd/include/emqttd_internal.hrl").

-import(string,[concat/2]).
-import(lists,[nth/2]). 


-export([load/1, unload/0]).
-export([on_message_publish/2]).

%% Called when the plugin application start
load(Env) ->
	ekaf_init([Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

on_message_publish(Message = #mqtt_message{topic = <<"ni/mtx/", _/binary>>},_Env) ->
    {ok, KTopic} = application:get_env(ekaf, mtxtopics),
    Payload = Message#mqtt_message.payload,
    ekaf:produce_async(KTopic, Payload),
    io:format("publish ~s~n", [emqttd_message:format(Message)]),
    {ok, Message};
on_message_publish(Message = #mqtt_message{topic = <<"ni/tx/", _/binary>>},_Env) ->
    {ok, KTopic} = application:get_env(ekaf, txtopics),
    Payload = Message#mqtt_message.payload,
    ekaf:produce_async(KTopic, Payload),
    io:format("publish ~s~n", [emqttd_message:format(Message)]),
    {ok, Message};
on_message_publish(Message,_Env) ->
    {ok, Message}.

ekaf_init(_Env) ->
    {ok, Kafka_Env} = application:get_env(?MODULE, server),
    Host = proplists:get_value(host, Kafka_Env),
    Port = proplists:get_value(port, Kafka_Env),
    TxTopic = proplists:get_value(txtopic, Kafka_Env),
    MtxTopic = proplists:get_value(mtxtopic, Kafka_Env),
    Broker = {Host, Port},
    application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
    application:set_env(ekaf, ekaf_buffer_ttl, 100),
    application:set_env(ekaf, txtopics, list_to_binary(TxTopic)),
    application:set_env(ekaf, mtxtopics, list_to_binary(MtxTopic)),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("Initialized ekaf with ~p~n", [{"localhost", 9092}]).

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2).
