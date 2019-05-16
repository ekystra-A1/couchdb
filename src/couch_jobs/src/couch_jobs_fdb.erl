% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_jobs_fdb).

-export([
    add/4,
    remove/3,
    resubmit/3,
    get_job/3,

    accept/2,
    accept/3,
    finish/5,
    resubmit/4,
    update/5,

    init_cache/0,

    get_jtx/0,
    get_jtx/1,

    tx/1
]).


% JobOpts field definitions
%
-define(OPT_PRIORITY, <<"priority">>).
-define(OPT_DATA, <<"data">>).
-define(OPT_CANCEL, <<"cancel">>).
-define(OPT_RESUBMIT, <<"resubmit">>).

% These might be in a fabric public .hrl eventually
%
-define(uint2bin(I), binary:encode_unsigned(I, little)).
-define(bin2uint(I), binary:decode_unsigned(I, little)).
-define(UNSET_VS, {versionstamp, 16#FFFFFFFFFFFFFFFF, 16#FFFF}).
-define(METADATA_VERSION_KEY, <<"$metadata_version_key$">>).

% Data model definitions
% Switch these to numbers eventually.
%
-define(JOBS, <<"couch_jobs">>).
-define(DATA, <<"data">>).
-define(PENDING, <<"pending">>).
-define(WATCHES, <<"watches">>).
-define(ACTIVITY_TIMEOUT, <<"activity_timeout">>).
-define(ACTIVITY, <<"activity">>).

%%% Data model %%%

%% (?JOBS, ?DATA, Type, JobId) = (Sequence, WorkerLockId, Priority, JobOpts)
%% (?JOBS, ?PENDING, Type, Priority, JobId) = ""
%% (?JOBS, ?WATCHES, Type) = Sequence
%% (?JOBS, ?ACTIVITY_TIMEOUT, Type) = ActivityTimeout
%% (?JOBS, ?ACTIVITY, Type, Sequence) = JobId


add(#{jtx := true} = JTx0, Type, JobId, JobOpts) ->
    #{tx := Tx, jobs_path := JobsPath} = JTx = get_jtx(JTx0),
    Key = pack({?DATA, Type, JobId}, JobsPath),
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        <<_/binary>> ->
            {error, duplicate_job};
        not_found ->
            Priority = maps:get(?OPT_PRIORITY, JobOpts, ?UNSET_VS),
            Val = pack({null, null, Priority, jiffy:encode(JobOpts)}),
            case has_versionstamp(Priority) of
                true -> erlfdb:set_versionstamped_value(Tx, Key, Val);
                false -> erlfdb:set(Tx, Key, Val)
            end,
            pending_enqueue(JTx, Type, Priority, JobId),
            ok
    end.


remove(#{jtx := true} = JTx0, Type, JobId) ->
    #{tx := Tx, jobs_path :=  JobsPath} = JTx = get_jtx(JTx0),
    Key = pack({?DATA, Type, JobId}, JobsPath),
    case get_job(Tx, Key) of
        {_, WorkerLockId, _, _} when WorkerLockId =/= null ->
            {error, job_is_running};
        {_, _, null, _,  _} ->
            erlfdb:clear(Tx, Key);
        {_, _, Priority, _, _} ->
            pending_remove(JTx, Type, Priority, JobId),
            erlfdb:clear(Tx, Key);
        not_found ->
            not_found
    end.


resubmit(#{jtx := true} = JTx, Type, JobId) ->
    #{tx := Tx, jobs_path :=  JobsPath} = get_jtx(JTx),
    Key = pack({?DATA, Type, JobId}, JobsPath),
    case get_job(Tx, Key) of
        {_, _, _, #{?OPT_RESUBMIT := true}} ->
            ok;
        {Seq, WorkerLockId, Priority, #{} = JobOpts} ->
            set_resubmit(Tx, Key, Seq, WorkerLockId, Priority, JobOpts);
        not_found ->
            not_found
    end.


get_job(#{jtx := true} = JTx, Type, JobId) ->
    #{tx := Tx, jobs_path :=  JobsPath} = get_jtx(JTx),
    Key = pack({?DATA, Type, JobId}, JobsPath),
    case get_job(Tx, Key) of
        {_, WorkerLockId, Priority, JobOpts} ->
            {ok, JobOpts, job_state(WorkerLockId, Priority)};
        not_found ->
            not_found
    end.


% Worker public API

accept(#{jtx := true} = JTx, Type) ->
    accept(JTx, Type, undefined).


accept(#{jtx := true} = JTx0, Type, MaxPriority) ->
    #{tx := Tx, jobs_path :=  JobsPath} = JTx = get_jtx(JTx0),
    PendingPrefix = pack({?PENDING, Type}, JobsPath),
    WorkerLockId = fabric2_util:uuid(),
    % Dequeue item from front of queue (use MaxPriority) for get_range
    % Create activity entry
    % Update sequence if jobs table
    % Update sequence in "watches"
    {ok, <<"AJobId">>,  WorkerLockId}.



finish(#{jtx := true} = JTx0, Type, JobId, JobOpts, WorkerLockId) ->
    #{tx := Tx, jobs_path :=  JobsPath} = JTx = get_jtx(JTx0),
    Key = pack({?DATA, Type, JobId}, JobsPath),
    try
        {Seq, _, _, JobOpts} = get_job_or_raise_status(Tx, Key, WorkerLockId),
        ActivityKey = pack({?ACTIVITY, Type, Seq}, JobsPath),
        erlfdb:clear(Tx, ActivityKey),
        case maps:get(?OPT_RESUBMIT, JobOpts, false) of
            true ->
                Priority = maps:get(?OPT_PRIORITY, JobOpts, ?UNSET_VS),
                JobOpts1 = maps:without([?OPT_PRIORITY], JobOpts),
                Val = pack({null, null, Priority, jiffy:encode(JobOpts1)}),
                case has_versionstamp(Priority) of
                    true -> erlfdb:set_versionstamped_value(Tx, Key, Val);
                    false -> erlfdb:set(Tx, Key, Val)
                end,
                pending_enqueue(JTx, Type, Priority, JobId);
            false ->
                Val = pack({null, null, null, jiffy:encode(JobOpts)}),
                erlfdb:set(Tx, Key, Val)
        end
    catch
        throw:worker_conflict -> worker_conflict;
        throw:canceled -> canceled
    end.


resubmit(#{jtx := true} = JTx, Type, JobId, WorkerLockId) ->
    #{tx := Tx, jobs_path :=  JobsPath} = get_jtx(JTx),
    Key = pack({?DATA, Type, JobId}, JobsPath),
    try
        case get_job_or_raise_status(Tx, Key, WorkerLockId) of
            {_, _, _, #{?OPT_RESUBMIT := true}} ->
                ok;
            {Seq, WorkerLockId, Priority, #{} = JobOpts} ->
                set_resubmit(Tx, Key, Seq, WorkerLockId, Priority, JobOpts)
        end
    catch
        throw:worker_conflict -> worker_conflict;
        throw:canceled -> canceled
    end.


update(#{jtx := true} = JTx, Type, JobId, JobOpts, WorkerLockId) ->
    #{tx := Tx, jobs_path :=  JobsPath} = get_jtx(JTx),
    % TODO
    ok. % worker_conflict | canceled


% Cache initialization API. Called from the supervisor just to create the ETS
% table. It returns `ignore` to tell supervisor it won't actually start any
% process, which is what we want here.
%
init_cache() ->
    ConcurrencyOpts = [{read_concurrency, true}, {write_concurrency, true}],
    ets:new(?MODULE, [public, named_table] ++ ConcurrencyOpts),
    ignore.


% Cached job transaction object. This object wraps a transaction, caches the
% directory lookup path, and the metadata version. The function can be used from
% or outside the transaction. When used from a transaction it will verify if
% the metadata was changed, and will refresh automatically.
%
get_jtx() ->
    get_jtx(undefined).


get_jtx(#{tx := Tx} = _TxDb) ->
    get_jtx(Tx);

get_jtx(undefined = _Tx) ->
    case ets:lookup(?MODULE, ?JOBS) of
        [{_, #{} = JTx}] -> JTx;
        [] -> update_jtx_cache(init_jtx(undefined))
    end;

get_jtx({erlfdb_transaction, _} = Tx) ->
    case ets:lookup(?MODULE, ?JOBS) of
        [{_, #{} = JTx}] -> ensure_current(JTx#{tx := Tx});
        [] -> update_jtx_cache(init_jtx(Tx))
    end.


% Private API helper functions


get_job(Tx = {erlfdb_transaction, _}, Key) ->
    case erlfdb:wait(erlfdb:get(Tx, Key)) of
        <<_/binary>> = Val ->
            {Seq, WorkerLockId, JobOptsBin} = unpack(Val),
            JobOpts = jiffy:decode(JobOptsBin, [return_maps]),
            {Seq, WorkerLockId, JobOpts};
        not_found ->
            not_found
    end.


get_job_or_raise_status(Tx, Key, WorkerLockId) ->
    case get_job(Tx, Key) of
        {_, CurWorkerLockId, _, _} when WorkerLockId =/= CurWorkerLockId ->
            throw(worker_conflict);
        {_, _, _, #{?OPT_CANCEL := true}} ->
            throw(canceled);
        {_, _, _, #{}} = Res ->
            Res
    end.


set_resubmit(Tx, Key, Seq, WorkerLockId, Priority, JobOpts0) ->
    JobOpts = JobOpts0#{?OPT_RESUBMIT => true},
    Val = pack({Seq, WorkerLockId, Priority, jiffy:encode(JobOpts)}),
    erlfdb:set(Tx, Key, Val).


pending_enqueue(#{jtx := true} = JTx, Type, Priority, JobId) ->
    #{tx := Tx, jobs_path := JobsPath} = JTx,
    Key = pack({?PENDING, Type, Priority, JobId}, JobsPath),
    case has_versionstamp(Priority) of
        true -> erlfdb:set_versionstamped_key(Tx, Key, null);
        false -> erlfdb:set(Tx, Key, null)
    end.

pending_remove(#{jtx := true} = JTx, Type, Priority, JobId) ->
    #{tx := Tx, jobs_path := JobsPath} = JTx,
    Key = pack({?PENDING, Type, Priority, JobId}, JobsPath),
    erlfdb:clear(Tx, Key).


has_versionstamp(?UNSET_VS) ->
    true;

has_versionstamp(Tuple) when is_tuple(Tuple) ->
    has_versionstamp(tuple_to_list(Tuple));

has_versionstamp([Elem | Rest]) ->
    has_versionstamp(Elem) orelse has_versionstamp(Rest);

has_versionstamp(_Other) ->
    false.


job_state(WorkerLockId, Priority) ->
    case {WorkerLockId, Priority} of
        {null, null} ->
            finished;
        {WorkerLockId, _} when WorkerLockId =/= null ->
            running;
        {_, Priority} when Priority =/= null ->
            pending;
        ErrorState ->
            error({invalid_job_state, ErrorState})
    end.


pack(Val) ->
    erlfdb_tuple:pack(Val).


pack(Val, Prefix) ->
    erlfdb_tuple:pack(Val, Prefix).


unpack(Val) ->
    erlfdb_tuple:unpack(Val).


unpack(Val, Prefix) ->
    erlfdb_tuple:unpack(Val, Prefix).


tx(Fun) when is_function(Fun, 1) ->
    fabric2_fdb:transactional(Fun).


tx(Tx, Fun) when is_function(Fun, 1) ->
    fabric2_fdb:transactional(Tx, Fun).


% This a transaction context object similar to the Db = #{} one from fabric2_fdb.
% It's is used to cache the jobs path directory (to avoid extra lookups on every
% operation) and to check for metadata changes (in case directory changes).
%
init_jtx(undefined) ->
    tx(fun(Tx) -> init_jtx(Tx) end);

init_jtx({erlfdb_transaction, _} = Tx) ->
    Root = erlfdb_directory:root(),
    CouchDB = erlfdb_directory:create_or_open(Tx, Root, [<<"couchdb">>]),
    LayerPrefix = erlfdb_directory:get_name(CouchDB),
    JobsPrefix = erlfdb_tuple:pack({?JOBS}, LayerPrefix),
    Version = erlfdb:wait(erlfdb:get(Tx, ?METADATA_VERSION_KEY)),
    % layer_prefix, md_version and tx here match db map fields in fabric2_fdb
    % but we also assert that this is a job transaction using the jtx => true field
    #{
        jtx => true,
        tx => Tx,
        layer_prefix => LayerPrefix,
        jobs_prefix => JobsPrefix,
        md_version => Version
    }.


ensure_current(#{jtx := true, tx := Tx, md_version := Version} = JTx) ->
    case erlfdb:wait(erlfdb:get(Tx, ?METADATA_VERSION_KEY)) of
        Version -> JTx;
        _NewVersion -> update_jtx_cache(init_jtx(Tx))
    end.


update_jtx_cache(#{jtx := true} = JTx) ->
    CachedJTx = JTx#{tx := undefined},
    ets:insert(?MODULE, {?JOBS, CachedJTx}),
    JTx.
