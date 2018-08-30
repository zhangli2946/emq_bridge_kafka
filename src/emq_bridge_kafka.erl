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
-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).

%% Called when the plugin application start
load(Env) ->
    io:format("Prev ekaf_init ===============================>~n"),
	ekaf_init([Env]),
    io:format("Post ekaf_init ===============================>~n"),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #mqtt_message{pktid   = PkgId,
                        qos     = Qos,
                        retain  = Retain,
                        dup     = Dup,
                        topic   = Topic,
                        payload = Payload
						}, _Env) ->
    io:format("publish ~s~n", [emqttd_message:format(Message)]),
    Str1 = <<"{\"topic\":\"">>,
    Str2 = <<"\", \"message\":[">>,
    Str3 = <<"]}">>,
    Str4 = <<Str1/binary, Topic/binary, Str2/binary, Payload/binary, Str3/binary>>,
	{ok,ProduceTopic}= application:get_key(kafka_producer_topic),
    ekaf:produce_async(ProduceTopic, Str4),	
    {ok, Message}.


on_message_delivered(ClientId, Username, Message, _Env) ->
    io:format("delivered to client(~s/~s): ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.

on_message_acked(ClientId, Username, Message, _Env) ->
    io:format("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.

ekaf_init(_Env) ->
    {ok, Kafka_Env} = application:get_env(?MODULE, server),
    Host = proplists:get_value(host, Kafka_Env),
    Port = proplists:get_value(port, Kafka_Env),
    Topic = proplists:get_value(topic, Kafka_Env),
    Broker = {Host, Port},
    application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
    application:set_env(ekaf, ekaf_buffer_ttl, 100),
    application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("Initialized ekaf with ~p~n", [{"localhost", 9092}]).

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/4),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/4).

