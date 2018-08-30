PROJECT = emq_bridge_kafka
PROJECT_DESCRIPTION = EMQTTD Kafka Bridge
PROJECT_VERSION = 2.0.x

DEPS = ekaf
dep_ekaf = git https://github.com/helpshift/ekaf master


BUILD_DEPS = emqttd cuttlefish
dep_emqttd = git https://github.com/emqtt/emqttd master
dep_cuttlefish = git https://github.com/emqtt/cuttlefish

COVER = true

include erlang.mk

app:: rebar.config

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emq_bridge_kafka.conf -i priv/emq_bridge_kafka.schema -d data
