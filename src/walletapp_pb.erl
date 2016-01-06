%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
%% @todo Add transaction like operations - buy voucher and reduce
%%       balance

-module(walletapp_pb).

-export([credit/3,
         debit/3,
         getbalance/2,
         buyvoucher/3,
         usevoucher/3,
         readvouchers/2,
         buyvoucher_from_wallet/4]).

-type key() :: term().
-type reason() :: atom().

-spec credit(key(), non_neg_integer(), pid()) -> ok | {error, reason()}.
credit(Key, Amount, Pid) ->
    BKey = {Key, riak_dt_pncounter, <<"bucket">>},
    Cnt = antidotec_counter:new(),
    Cnt2 = antidotec_counter:increment(Amount, Cnt),
    case  antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}) of
        {ok, TxId} ->
            ok = antidotec_pb:update_objects(Pid,
                                             antidotec_counter:to_ops(BKey, Cnt2),
                                             TxId),
            case antidotec_pb:commit_transaction(Pid, TxId) of
                {ok, _} ->  ok;
                _ ->  {error, commit_failed}
            end;
        _ ->
            {error, start_txn_failed}
    end.

-spec debit(key(), non_neg_integer(), pid()) -> ok | {error, reason()}.
debit(Key, Amount, Pid) ->
    BKey = {Key, riak_dt_pncounter, <<"bucket">>},
    Cnt = antidotec_counter:new(),
    Cnt2 = antidotec_counter:decrement(Amount, Cnt),
    case  antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}) of
        {ok, TxId} ->
            ok = antidotec_pb:update_objects(Pid,
                                             antidotec_counter:to_ops(BKey, Cnt2),
                                             TxId),
            case antidotec_pb:commit_transaction(Pid, TxId) of
                {ok, _} ->  ok;
                _ ->  {error, commit_failed}
            end;
        _ ->
            {error, start_txn_failed}
    end.

-spec getbalance(key(), pid()) -> {error, reason()} | {ok, integer()}.
getbalance(Key, Pid) ->
    BKey = {Key, riak_dt_pncounter, <<"bucket">>},
    case antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}) of
        {ok, TxId} ->
            case antidotec_pb:read_objects(Pid,
                                           [BKey],
                                           TxId) of
                {ok, [Bal]} -> 
                    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
                    {ok, antidotec_counter:value(Bal)};
                _ ->
                    {error, error_in_read}
            end;
        _ ->
            {error, start_txn_failed}
    end.

-spec buyvoucher(key(), term(), pid()) -> ok | {error, reason()}.
buyvoucher(Key, Voucher, Pid) ->
    BKey = {Key, riak_dt_orset, <<"bucket">>},
    VSet = antidotec_set:new(),
    VSet2 = antidotec_set:add(Voucher, VSet),

    case  antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}) of
        {ok, TxId} ->
            ok = antidotec_pb:update_objects(Pid,
                                             antidotec_set:to_ops(BKey, VSet2),
                                             TxId),
            case antidotec_pb:commit_transaction(Pid, TxId) of
                {ok, _} ->  ok;
                _ ->  {error, commit_failed}
            end;
        _ ->
            {error, start_txn_failed}
    end.

-spec usevoucher(key(), term(), pid()) -> ok | {error, reason()}.
usevoucher(Key, Voucher, Pid) ->
    BKey = {Key, riak_dt_orset, <<"bucket">>},
    VSet = antidotec_set:new(),
    VSet2 = antidotec_set:remove(Voucher, VSet),
    case  antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}) of
        {ok, TxId} ->
            ok = antidotec_pb:update_objects(Pid,
                                             antidotec_set:to_ops(BKey, VSet2),
                                             TxId),
            case antidotec_pb:commit_transaction(Pid, TxId) of
                {ok, _} ->  ok;
                _ ->  {error, commit_failed}
            end;
        _ ->
            {error, start_txn_failed}
    end.

-spec readvouchers(key(), pid()) -> {ok, list()} | {error, reason()}.
readvouchers(Key, Pid) ->
    BKey = {Key, riak_dt_orset, <<"bucket">>},
    case antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}) of
        {ok, TxId} ->
            case antidotec_pb:read_objects(Pid,
                                           [BKey],
                                           TxId) of
                {ok, [Val]} -> 
                    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
                    {ok, antidotec_set:value(Val)};
                _ ->
                    {error, error_in_read}
            end;
        _ ->
            {error, start_txn_failed}
    end.

buyvoucher_from_wallet(KeyW, KeyV, Voucher, Pid) ->
    WKey = {KeyW, riak_dt_pncounter, <<"bucket">>},
    Wallet = antidotec_counter:decrement(1, antidotec_counter:new()),
    
    VKey = {KeyV, riak_dt_orset, <<"bucket">>},
    VSet = antidotec_set:add(Voucher, antidotec_set:new()),

    {ok, TxId} = antidotec_pb:start_transaction(Pid, term_to_binary(ignore), {}),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_counter:to_ops(WKey, Wallet),
                                     TxId),
    ok = antidotec_pb:update_objects(Pid,
                                     antidotec_set:to_ops(VKey, VSet),
                                     TxId),
    
    {ok, _} = antidotec_pb:commit_transaction(Pid, TxId),
    ok.
