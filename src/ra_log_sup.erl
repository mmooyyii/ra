%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link(ra_system:config()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(#{names := #{log_sup := Name}} = Cfg) ->
    supervisor:start_link({local, Name}, ?MODULE, [Cfg]).

init([#{data_dir := DataDir,
        name := System,
        names := #{wal := WalName,
                   segment_writer := SegWriterName} = Names} = Cfg]) ->
    %% TODO: make unnamed
    PreInit = #{id => ra_log_pre_init,
                start => {ra_log_pre_init, start_link, [System]}},
    Meta = #{id => ra_log_meta,
             start => {ra_log_meta, start_link, [Cfg]}},
    SegmentMaxEntries = application:get_env(ra, segment_max_entries, 4096),
    SegWriterConf = #{name => SegWriterName,
                      system => System,
                      data_dir => DataDir,
                      segment_conf => #{max_count => SegmentMaxEntries}},
    SegWriter = #{id => ra_log_segment_writer,
                  start => {ra_log_segment_writer, start_link,
                            [SegWriterConf]}},
    WalDir = case Cfg of
                 #{wal_data_dir := D} -> D;
                 _ -> DataDir
             end,
    WalConf = #{name => WalName,
                names => Names,
                dir => WalDir,
                segment_writer => SegWriterName},
    SupFlags = #{strategy => one_for_all, intensity => 5, period => 5},
    WalSup = #{id => ra_log_wal_sup,
               type => supervisor,
               start => {ra_log_wal_sup, start_link, [WalConf]}},
    {ok, {SupFlags, [PreInit, Meta, SegWriter, WalSup]}}.
