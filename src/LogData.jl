# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module LogData

using ElasticArrays

export  Log, get_field, get_record, log, num_records, start_new_record


# row always points to an empty slot
mutable struct Log
    nfields
    num_preallocated_records
    current_record
    data
    header
end

function Log(header; len = 10000)
    nfields = length(header)
    num_preallocated_records = 1000
    data = ElasticArray{Float64}(undef, nfields, num_preallocated_records)
    sizehint!(data, nfields, len * nfields)
    log = Log(nfields, num_preallocated_records, 1, data, header)
end

# increment the current column
function start_new_record(logobj::Log)
    logobj.current_record += 1
    if logobj.current_record > num_allocated_records(logobj)
        N = size(logobj.data, 2) + logobj.num_preallocated_records
        resize!(logobj.data, logobj.nfields, N)
    end
end

# write to a specific row in the current column
function log(logobj::Log, fieldnum, val)
    record = logobj.current_record
    logobj.data[fieldnum, record] = val
    return nothing
end


num_allocated_records(a::Log) = size(a.data, 2)
num_records(logobj::Log) = logobj.current_record-1
get_record(logobj::Log, k) = logobj.data(:,k)

# return a single row of the data
function get_field(logobj::Log, fieldname)
    N = num_records(logobj)
    field_row = logobj.header[fieldname]
    return logobj.data[field_row, 1:N]
end






end
