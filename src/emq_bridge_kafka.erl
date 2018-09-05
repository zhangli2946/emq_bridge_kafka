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

get_form_clientid({ClientId, Username}) -> ClientId;
get_form_clientid(From) -> From.
get_form_username({ClientId, Username}) -> Username;
get_form_username(From) -> From.

% on_message_publish(Message,_Env) ->
%     From = Message#mqtt_message.from, 
%     Topic = Message#mqtt_message.topic,
%     Payload = Message#mqtt_message.payload,
%     Qos = Message#mqtt_message.qos,
%     Dup = Message#mqtt_message.dup,
%     Retain = Message#mqtt_message.retain,
%     {ok, KTopic} = application:get_env(ekaf, ekaf_bootstrap_topics),
%     ClientId = get_form_clientid(From),
%     Username = get_form_username(From),
%     Json = mochijson2:encode([
%         {client_id, ClientId},
%         {message, [
%             {username, Username},
%             {topic, Topic},
%             {payload, Payload},
%             {qos, Qos},
%             {dup, Dup},
%             {retain, Retain}
%         ]},
%         {cluster_node, node()},
%         {ts, emqttd_time:now_ms()}
%     ]),
%     ekaf:produce_async(KTopic, list_to_binary(Json)),
%     io:format("publish ~s~n", [emqttd_message:format(Message)]),
%     {ok, Message}.

on_message_publish(Message = #mqtt_message{topic = <<"ni/rx/", _/binary>>},_Env) ->
        {ok, KTopic} = application:get_env(ekaf, rxtopics),
        Topic = Message#mqtt_message.topic,
        Payload = Message#mqtt_message.payload,
        Json = mochijson2:encode([
            {topic, Topic},
            {payload, Payload}
        ]),
        ekaf:produce_async(KTopic, list_to_binary(Json)),
        io:format("publish ~s~n", [emqttd_message:format(Message)]),
        {ok, Message}.

ekaf_init(_Env) ->
    {ok, Kafka_Env} = application:get_env(?MODULE, server),
    Host = proplists:get_value(host, Kafka_Env),
    Port = proplists:get_value(port, Kafka_Env),
    Topic = proplists:get_value(topic, Kafka_Env),
    RxTopic = proplists:get_value(rxtopic, Kafka_Env),
    Broker = {Host, Port},
    application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
    application:set_env(ekaf, ekaf_buffer_ttl, 100),
    application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
    application:set_env(ekaf, rxtopics, list_to_binary(RxTopic)),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("Initialized ekaf with ~p~n", [{"localhost", 9092}]).

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2).
