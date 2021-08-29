// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_SRC_QUERY_EXEC_OLAP_SCANNER_H
#define DORIS_BE_SRC_QUERY_EXEC_OLAP_SCANNER_H

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/exec_node.h"
#include "exec/olap_utils.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/expr.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/delete_handler.h"
#include "olap/olap_cond.h"
#include "olap/reader.h"
#include "olap/rowset/column_data.h"
#include "olap/storage_engine.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/vectorized_row_batch.h"

namespace doris {

class OlapScanNode;
class OLAPReader;
class RuntimeProfile;
class Field;
/*
1. OlapScanner对一个tablet(可以理解为chunk)数据读取操作整体的封装
2. Reader对读取的参数进行处理，并提供了按三种不同模型读取的差异化处理。
3. CollectIterator包含了tablet中多个RowsetReader，这些RowsetReader有版本顺序，
    CollectIterator将这些RowsetReader归并Merge成统一的Iterator功能，提供了归并的
    比较器。
4. RowsetReader则负责了对一个Rowset的读取。
5. RowwiseIterator提供了一个Rowset中所有Segment的统一访问的Iterator功能。这里
    的归并策略可以根据数据排序的情况采用Merge或Union。
6. SegmentIterator对应了一个Segment的数据读取，Segment的读取会根据查询条件与
    索引进行计算找到读取的对应行号信息，seek到对应的page，对数据进行读取。其
    中，经过过滤条件后会对可访问的行信息生成bitmap来记录，BitmapRangeIterator
    为单独实现的可以按照范围访问这个bitmap的迭代器。
7. ColumnIterator提供了对列的相关数据和索引统一访问的迭代器。ColumnReader、
    各个IndexReader等对应了具体的数据和索引信息的读取。
*/
class OlapScanner {
public:
    OlapScanner(RuntimeState* runtime_state, OlapScanNode* parent, bool aggregation,
                bool need_agg_finalize, const TPaloScanRange& scan_range,
                const std::vector<OlapScanRange*>& key_ranges);

    ~OlapScanner();

    Status prepare(const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
                   const std::vector<TCondition>& filters,
                   const std::vector<std::pair<std::string, std::shared_ptr<IBloomFilterFuncBase>>>&
                           bloom_filters);

    Status open();

    Status get_batch(RuntimeState* state, RowBatch* batch, bool* eof);

    Status close(RuntimeState* state);

    RuntimeState* runtime_state() { return _runtime_state; }

    std::vector<ExprContext*>* conjunct_ctxs() { return &_conjunct_ctxs; }

    int id() const { return _id; }
    void set_id(int id) { _id = id; }
    bool is_open() const { return _is_open; }
    void set_opened() { _is_open = true; }

    int64_t raw_rows_read() const { return _raw_rows_read; }

    void update_counter();

    const std::string& scan_disk() const { return _tablet->data_dir()->path(); }

    void start_wait_worker_timer() {
        _watcher.reset();
        _watcher.start();
    }

    int64_t update_wait_worker_timer() { return _watcher.elapsed_time(); }

    void set_use_pushdown_conjuncts(bool has_pushdown_conjuncts) {
        _use_pushdown_conjuncts = has_pushdown_conjuncts;
    }

    std::vector<bool>* mutable_runtime_filter_marks() { return &_runtime_filter_marks; }

private:
    Status _init_params(const std::vector<OlapScanRange*>& key_ranges,
                        const std::vector<TCondition>& filters,
                        const std::vector<std::pair<string, std::shared_ptr<IBloomFilterFuncBase>>>&
                                bloom_filters);
    Status _init_return_columns();
    void _convert_row_to_tuple(Tuple* tuple);

    // Update profile that need to be reported in realtime.
    void _update_realtime_counter();

private:
    RuntimeState* _runtime_state;
    OlapScanNode* _parent;
    const TupleDescriptor* _tuple_desc; /**< tuple descriptor */
    RuntimeProfile* _profile;
    const std::vector<SlotDescriptor*>& _string_slots;
    const std::vector<SlotDescriptor*>& _collection_slots;

    std::vector<ExprContext*> _conjunct_ctxs;
    // to record which runtime filters have been used
    std::vector<bool> _runtime_filter_marks;

    int _id;
    bool _is_open;
    bool _aggregation;
    bool _need_agg_finalize = true;
    bool _has_update_counter = false;

    int _tuple_idx = 0;
    int _direct_conjunct_size = 0;

    bool _use_pushdown_conjuncts = false;

    ReaderParams _params;
    std::unique_ptr<Reader> _reader;

    TabletSharedPtr _tablet;
    int64_t _version;

    std::vector<uint32_t> _return_columns;

    RowCursor _read_row_cursor;

    std::vector<SlotDescriptor*> _query_slots;

    // time costed and row returned statistics
    ExecNode::EvalConjunctsFn _eval_conjuncts_fn = nullptr;

    RuntimeProfile::Counter* _rows_read_counter = nullptr;
    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _compressed_bytes_read = 0;

    RuntimeProfile::Counter* _rows_pushed_cond_filtered_counter = nullptr;
    // number rows filtered by pushed condition
    int64_t _num_rows_pushed_cond_filtered = 0;

    bool _is_closed = false;

    MonotonicStopWatch _watcher;

    std::shared_ptr<MemTracker> _mem_tracker;
};

} // namespace doris

#endif
