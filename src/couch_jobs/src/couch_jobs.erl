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

-module(couch_jobs).

-export([
    add/3,
    remove/2,
    resubmit/2,
    get_job/2,

    accept/1,
    accept/2,
    finish/5,
    resubmit/4,
    update/5
]).


%% Job Creation API

add(Type, JobId, JobOpts) ->
    try
        ok = validate_jobopts(JobOpts)
    catch
        Tag:Err -> {error, {invalid_job_args, Tag, Err}}
    end,
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_job_fdb:add(JTx, Type, JobId, JobOpts)
    end).


remove(Type, JobId) ->
    % Add the bit about cancelling the job if it is running
    % and waiting for it to stop, then remove it.
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:remove(JTx, Type, JobId)
    end).


resubmit(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Type, JobId)
    end).


get_job(Type, JobId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:get_job(JTx, Type, JobId)
    end).


%% Worker Implementation API

accept(Type) ->
    accept(Type, undefined).


accept(Type, MaxPriority) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(), fun(JTx) ->
        couch_jobs_fdb:accept(JTx, Type, MaxPriority)
    end).


finish(Tx, Type, JobId, JobOpts, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:finish(JTx, Type, JobId, JobOpts, WorkerLockId)
    end).


resubmit(Tx, Type, JobId, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:resubmit(JTx, Type, JobId, WorkerLockId)
    end).


update(Tx, Type, JobId, JobOpts, WorkerLockId) ->
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        couch_jobs_fdb:update(JTx, Type, JobId, JobOpts, WorkerLockId)
    end).


%% Private utils

validate_jobopts(#{} = JobOpts) ->
    jiffy:encode(JobOpts),
    ok.
