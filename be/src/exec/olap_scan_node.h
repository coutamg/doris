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

#ifndef DORIS_BE_SRC_QUERY_EXEC_OLAP_SCAN_NODE_H
#define DORIS_BE_SRC_QUERY_EXEC_OLAP_SCAN_NODE_H

#include <atomic>
#include <boost/thread.hpp>
#include <boost/variant/static_visitor.hpp>
#include <condition_variable>
#include <queue>

#include "exec/olap_common.h"
#include "exec/olap_scanner.h"
#include "exec/scan_node.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/in_predicate.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch_interface.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/progress_updater.h"
#include "util/spinlock.h"

namespace doris {
class IRuntimeFilter;

enum TransferStatus {
    READ_ROWBATCH = 1,
    INIT_HEAP = 2,
    BUILD_ROWBATCH = 3,
    MERGE = 4,
    FINISH = 5,
    ADD_ROWBATCH = 6,
    ERROR = 7
};

/*
	参考: http://doris.apache.org/master/zh-CN/administrator-guide/running-profile.html#profile%E5%8F%82%E6%95%B0%E8%A7%A3%E6%9E%90

	OLAP_SCAN_NODE 节点负责具体的数据扫描任务。一个 OLAP_SCAN_NODE 会生成一个或多个
	OlapScanner 。每个 Scanner 线程负责扫描部分数据.

	查询中的部分或全部谓词条件会推送给 OLAP_SCAN_NODE。这些谓词条件中一部分会继续下推给
	存储引擎，以便利用存储引擎的索引进行数据过滤。另一部分会保留在 OLAP_SCAN_NODE 中，
	用于过滤从存储引擎中返回的数据.

	OLAP_SCAN_NODE 节点的 Profile 通常用于分析数据扫描的效率，依据调用关系分为
	OLAP_SCAN_NODE、OlapScanner、SegmentIterator 三层.

	一个典型的 OLAP_SCAN_NODE 节点的 Profile 如下。部分指标会因存储格式的不同（V1 或 V2）
	而有不同含义:

	OLAP_SCAN_NODE (id=0):(Active: 1.2ms, % non-child: 0.00%)
	- BytesRead: 265.00 B                 # 从数据文件中读取到的数据量。假设读取到了是10个32位整型，则数据量为 10 * 4B = 40 Bytes。这个数据仅表示数据在内存中全展开的大小，并不代表实际的 IO 大小。 
	- NumDiskAccess: 1                    # 该 ScanNode 节点涉及到的磁盘数量。
	- NumScanners: 20                     # 该 ScanNode 生成的 Scanner 数量。
	- PeakMemoryUsage: 0.00               # 查询时内存使用的峰值，暂未使用
	- RowsRead: 7                         # 从存储引擎返回到 Scanner 的行数，不包括经 Scanner 过滤的行数。
	- RowsReturned: 7                     # 从 ScanNode 返回给上层节点的行数。
	- RowsReturnedRate: 6.979K /sec       # RowsReturned/ActiveTime
	- TabletCount : 20                    # 该 ScanNode 涉及的 Tablet 数量。
	- TotalReadThroughput: 74.70 KB/sec   # BytesRead除以该节点运行的总时间（从Open到Close），对于IO受限的查询，接近磁盘的总吞吐量。
	- ScannerBatchWaitTime: 426.886us     # 用于统计transfer 线程等待scaner 线程返回rowbatch的时间。
	- ScannerWorkerWaitTime: 17.745us     # 用于统计scanner thread 等待线程池中可用工作线程的时间。
	OlapScanner:
		- BlockConvertTime: 8.941us         # 将向量化Block转换为行结构的 RowBlock 的耗时。向量化 Block 在 V1 中为 VectorizedRowBatch，V2中为 RowBlockV2。
		- BlockFetchTime: 468.974us         # Rowset Reader 获取 Block 的时间。
		- ReaderInitTime: 5.475ms           # OlapScanner 初始化 Reader 的时间。V1 中包括组建 MergeHeap 的时间。V2 中包括生成各级 Iterator 并读取第一组Block的时间。
		- RowsDelFiltered: 0                # 包括根据 Tablet 中存在的 Delete 信息过滤掉的行数，以及 unique key 模型下对被标记的删除行过滤的行数。
		- RowsPushedCondFiltered: 0         # 根据传递下推的谓词过滤掉的条件，比如 Join 计算中从 BuildTable 传递给 ProbeTable 的条件。该数值不准确，因为如果过滤效果差，就不再过滤了。
		- ScanTime: 39.24us                 # 从 ScanNode 返回给上层节点的行数。
		- ShowHintsTime_V1: 0ns             # V2 中无意义。V1 中读取部分数据来进行 ScanRange 的切分。
		SegmentIterator:
		- BitmapIndexFilterTimer: 779ns   # 利用 bitmap 索引过滤数据的耗时。
		- BlockLoadTime: 415.925us        # SegmentReader(V1) 或 SegmentIterator(V2) 获取 block 的时间。
		- BlockSeekCount: 12              # 读取 Segment 时进行 block seek 的次数。
		- BlockSeekTime: 222.556us        # 读取 Segment 时进行 block seek 的耗时。
		- BlocksLoad: 6                   # 读取 Block 的数量
		- CachedPagesNum: 30              # 仅 V2 中，当开启 PageCache 后，命中 Cache 的 Page 数量。
		- CompressedBytesRead: 0.00       # V1 中，从文件中读取的解压前的数据大小。V2 中，读取到的没有命中 PageCache 的 Page 的压缩前的大小。
		- DecompressorTimer: 0ns          # 数据解压耗时。
		- IOTimer: 0ns                    # 实际从操作系统读取数据的 IO 时间。
		- IndexLoadTime_V1: 0ns           # 仅 V1 中，读取 Index Stream 的耗时。
		- NumSegmentFiltered: 0           # 在生成 Segment Iterator 时，通过列统计信息和查询条件，完全过滤掉的 Segment 数量。
		- NumSegmentTotal: 6              # 查询涉及的所有 Segment 数量。
		- RawRowsRead: 7                  # 存储引擎中读取的原始行数。详情见下文。
		- RowsBitmapIndexFiltered: 0      # 仅 V2 中，通过 Bitmap 索引过滤掉的行数。
		- RowsBloomFilterFiltered: 0      # 仅 V2 中，通过 BloomFilter 索引过滤掉的行数。
		- RowsKeyRangeFiltered: 0         # 仅 V2 中，通过 SortkeyIndex 索引过滤掉的行数。
		- RowsStatsFiltered: 0            # V2 中，通过 ZoneMap 索引过滤掉的行数，包含删除条件。V1 中还包含通过 BloomFilter 过滤掉的行数。
		- RowsConditionsFiltered: 0       # 仅 V2 中，通过各种列索引过滤掉的行数。
		- RowsVectorPredFiltered: 0       # 通过向量化条件过滤操作过滤掉的行数。
		- TotalPagesNum: 30               # 仅 V2 中，读取的总 Page 数量。
		- UncompressedBytesRead: 0.00     # V1 中为读取的数据文件解压后的大小（如果文件无需解压，则直接统计文件大小）。V2 中，仅统计未命中 PageCache 的 Page 解压后的大小（如果Page无需解压，直接统计Page大小）
		- VectorPredEvalTime: 0ns         # 向量化条件过滤操作的耗时。

	通过 Profile 中数据行数相关指标可以推断谓词条件下推和索引使用情况。以下仅针对 Segment V2 格式数据读取流程中的 Profile 进行说明。Segment V1 格式中，这些指标的含义略有不同。

	当读取一个 V2 格式的 Segment 时，若查询存在 key_ranges（前缀key组成的查询范围），首先通过 SortkeyIndex 索引过滤数据，过滤的行数记录在 RowsKeyRangeFiltered。

	之后，对查询条件中含有 bitmap 索引的列，使用 Bitmap 索引进行精确过滤，过滤的行数记录在 RowsBitmapIndexFiltered。

	之后，按查询条件中的等值（eq，in，is）条件，使用BloomFilter索引过滤数据，记录在 RowsBloomFilterFiltered。RowsBloomFilterFiltered 的值是 Segment 的总行数（而不是Bitmap索引过滤后的行数）和经过 BloomFilter 过滤后剩余行数的差值，因此 BloomFilter 过滤的数据可能会和 Bitmap 过滤的数据有重叠。

	之后，按查询条件和删除条件，使用 ZoneMap 索引过滤数据，记录在 RowsStatsFiltered。

	RowsConditionsFiltered 是各种索引过滤的行数，包含了 RowsBloomFilterFiltered 和 RowsStatsFiltered 的值。
	至此 Init 阶段完成，Next 阶段删除条件过滤的行数，记录在 RowsDelFiltered。因此删除条件实际过滤的行数，分别记录在 RowsStatsFiltered 和 RowsDelFiltered 中。

	RawRowsRead 是经过上述过滤后，最终需要读取的行数。

	RowsRead 是最终返回给 Scanner 的行数。RowsRead 通常小于 RawRowsRead，是因为从存储引擎返回到 Scanner，可能会经过一次数据聚合。如果 RawRowsRead 和 RowsRead 差距较大，则说明大量的行被聚合，而聚合可能比较耗时。

	RowsReturned 是 ScanNode 最终返回给上层节点的行数。RowsReturned 通常也会小于RowsRead。因为在 Scanner 上会有一些没有下推给存储引擎的谓词条件，会进行一次过滤。如果 RowsRead 和 RowsReturned 差距较大，则说明很多行在 Scanner 中进行了过滤。这说明很多选择度高的谓词条件并没有推送给存储引擎。而在 Scanner 中的过滤效率会比在存储引擎中过滤效率差。

	通过以上指标，可以大致分析出存储引擎处理的行数以及最终过滤后的结果行数大小。通过 Rows***Filtered 这组指标，也可以分析查询条件是否下推到了存储引擎，以及不同索引的过滤效果。此外还可以通过以下几个方面进行简单的分析。

	OlapScanner 下的很多指标，如 IOTimer，BlockFetchTime 等都是所有 Scanner 线程指标的累加，因此数值可能会比较大。并且因为 Scanner 线程是异步读取数据的，所以这些累加指标只能反映 Scanner 累加的工作时间，并不直接代表 ScanNode 的耗时。ScanNode 在整个查询计划中的耗时占比为 Active 字段记录的值。有时会出现比如 IOTimer 有几十秒，而 Active 实际只有几秒钟。这种情况通常因为：

	IOTimer 为多个 Scanner 的累加时间，而 Scanner 数量较多。

	上层节点比较耗时。比如上层节点耗时 100秒，而底层 ScanNode 只需 10秒。则反映在 Active 的字段可能只有几毫秒。因为在上层处理数据的同时，ScanNode 已经异步的进行了数据扫描并准备好了数据。当上层节点从 ScanNode 获取数据时，可以获取到已经准备好的数据，因此 Active 时间很短。

	NumScanners 表示 Scanner 提交到线程池的Task个数，由 RuntimeState 中的线程池调度，doris_scanner_thread_pool_thread_num 和 doris_scanner_thread_pool_queue_size 两个参数分别控制线程池的大小和队列长度。线程数过多或过少都会影响查询效率。同时可以用一些汇总指标除以线程数来大致的估算每个线程的耗时。

	TabletCount 表示需要扫描的 tablet 数量。数量过多可能意味着需要大量的随机读取和数据合并操作。

	UncompressedBytesRead 间接反映了读取的数据量。如果该数值较大，说明可能有大量的 IO 操作。
	
	CachedPagesNum 和 TotalPagesNum 可以查看命中 PageCache 的情况。命中率越高，说明 IO 和解压操作耗时越少。

*/
class OlapScanNode : public ScanNode {
public:
    OlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~OlapScanNode();
    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    Status collect_query_statistics(QueryStatistics* statistics) override;
    virtual Status close(RuntimeState* state);
    virtual Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges);
    inline void set_no_agg_finalize() { _need_agg_finalize = false; }

protected:
    typedef struct {
        Tuple* tuple;
        int id;
    } HeapType;
    class IsFixedValueRangeVisitor : public boost::static_visitor<bool> {
    public:
        template <class T>
        bool operator()(T& v) const {
            return v.is_fixed_value_range();
        }
    };

    class GetFixedValueSizeVisitor : public boost::static_visitor<size_t> {
    public:
        template <class T>
        size_t operator()(T& v) const {
            return v.get_fixed_value_size();
        }
    };

    class ExtendScanKeyVisitor : public boost::static_visitor<Status> {
    public:
        ExtendScanKeyVisitor(OlapScanKeys& scan_keys, int32_t max_scan_key_num)
                : _scan_keys(scan_keys), _max_scan_key_num(max_scan_key_num) {}
        template <class T>
        Status operator()(T& v) {
            return _scan_keys.extend_scan_key(v, _max_scan_key_num);
        }

    private:
        OlapScanKeys& _scan_keys;
        int32_t _max_scan_key_num;
    };

    typedef boost::variant<std::list<std::string>> string_list;

    class ToOlapFilterVisitor : public boost::static_visitor<void> {
    public:
        template <class T, class P>
        void operator()(T& v, P& v2) const {
            v.to_olap_filter(v2);
        }
    };

    class MergeComparison {
    public:
        MergeComparison(CompareLargeFunc compute_fn, int offset) {
            _compute_fn = compute_fn;
            _offset = offset;
        }
        bool operator()(const HeapType& lhs, const HeapType& rhs) const {
            return (*_compute_fn)(lhs.tuple->get_slot(_offset), rhs.tuple->get_slot(_offset));
        }

    private:
        CompareLargeFunc _compute_fn;
        int _offset;
    };

    typedef std::priority_queue<HeapType, std::vector<HeapType>, MergeComparison> Heap;

    void display_heap(Heap& heap) {
        Heap h = heap;
        std::stringstream s;
        s << "Heap: [";

        while (!h.empty()) {
            HeapType v = h.top();
            s << "\nID: " << v.id << " Value:" << Tuple::to_string(v.tuple, *_tuple_desc);
            h.pop();
        }

        VLOG_CRITICAL << s.str() << "\n]";
    }

    // In order to ensure the accuracy of the query result
    // only key column conjuncts will be remove as idle conjunct
    bool is_key_column(const std::string& key_name);
    void remove_pushed_conjuncts(RuntimeState* state);

    Status start_scan(RuntimeState* state);

    void eval_const_conjuncts();
    Status normalize_conjuncts();
    Status build_olap_filters();
    Status build_scan_key();
    Status start_scan_thread(RuntimeState* state);

    template <class T>
    Status normalize_predicate(ColumnValueRange<T>& range, SlotDescriptor* slot);

    template <class T>
    Status normalize_in_and_eq_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range);

    template <class T>
    Status normalize_not_in_and_not_eq_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range);

    template <class T>
    Status normalize_noneq_binary_predicate(SlotDescriptor* slot, ColumnValueRange<T>* range);

    Status normalize_bloom_filter_predicate(SlotDescriptor* slot);

    template <typename T>
    static bool normalize_is_null_predicate(Expr* expr, SlotDescriptor* slot,
                                            const std::string& is_null_str,
                                            ColumnValueRange<T>* range);

    void transfer_thread(RuntimeState* state);
    void scanner_thread(OlapScanner* scanner);

    Status add_one_batch(RowBatchInterface* row_batch);

    // Write debug string of this into out.
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() { return _runtime_filter_descs; }

private:
    void _init_counter(RuntimeState* state);
    // OLAP_SCAN_NODE profile layering: OLAP_SCAN_NODE, OlapScanner, and SegmentIterator
    // according to the calling relationship
    void init_scan_profile();

    bool should_push_down_in_predicate(SlotDescriptor* slot, InPredicate* in_pred);

    template <typename T, typename ChangeFixedValueRangeFunc>
    static Status change_fixed_value_range(ColumnValueRange<T>& range, PrimitiveType type,
                                           void* value, const ChangeFixedValueRangeFunc& func);

    std::pair<bool, void*> should_push_down_eq_predicate(SlotDescriptor* slot, Expr* pred,
                                                         int conj_idx, int child_idx);

    friend class OlapScanner;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    // doris scan node used to scan doris
    TOlapScanNode _olap_scan_node;
    // tuple descriptors
    const TupleDescriptor* _tuple_desc;
    // tuple index
    int _tuple_idx;
    // string slots
    std::vector<SlotDescriptor*> _string_slots;
    // conjunct's index which already be push down storage engine
    // should be remove in olap_scan_node, no need check this conjunct again
    std::set<uint32_t> _pushed_conjuncts_index;
    // collection slots
    std::vector<SlotDescriptor*> _collection_slots;

    bool _eos;

    // column -> ColumnValueRange map
    std::map<std::string, ColumnValueRangeType> _column_value_ranges;

    OlapScanKeys _scan_keys;

    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;

    std::vector<TCondition> _olap_filter;
    // push down bloom filters to storage engine.
    // 1. std::pair.first :: column name
    // 2. std::pair.second :: shared_ptr of BloomFilterFuncBase
    std::vector<std::pair<std::string, std::shared_ptr<IBloomFilterFuncBase>>>
            _bloom_filters_push_down;

    // Pool for storing allocated scanner objects.  We don't want to use the
    // runtime pool to ensure that the scanner objects are deleted before this
    // object is.
    std::unique_ptr<ObjectPool> _scanner_pool;

    boost::thread_group _transfer_thread;

    // Keeps track of total splits and the number finished.
    ProgressUpdater _progress;

    // Lock and condition variables protecting _materialized_row_batches.  Row batches are
    // produced asynchronously by the scanner threads and consumed by the main thread in
    // GetNext.  Row batches must be processed by the main thread in the order they are
    // queued to avoid freeing attached resources prematurely (row batches will never depend
    // on resources attached to earlier batches in the queue).
    // This lock cannot be taken together with any other locks except _lock.
    std::mutex _row_batches_lock;
    std::condition_variable _row_batch_added_cv;
    std::condition_variable _row_batch_consumed_cv;

    std::list<RowBatchInterface*> _materialized_row_batches;

    std::mutex _scan_batches_lock;
    std::condition_variable _scan_batch_added_cv;
    int64_t _running_thread = 0;
    std::condition_variable _scan_thread_exit_cv;

    std::list<RowBatchInterface*> _scan_row_batches;

    std::list<OlapScanner*> _olap_scanners;

    int _max_materialized_row_batches;
    bool _start;
    // Used in Scan thread to ensure thread-safe
    std::atomic_bool _scanner_done;
    std::atomic_bool _transfer_done;
    size_t _direct_conjunct_size;

    int _total_assign_num;
    int _nice;

    // protect _status, for many thread may change _status
    SpinLock _status_mutex;
    Status _status;
    RuntimeState* _runtime_state;
    RuntimeProfile::Counter* _scan_timer;
    RuntimeProfile::Counter* _scan_cpu_timer = nullptr;
    RuntimeProfile::Counter* _tablet_counter;
    RuntimeProfile::Counter* _rows_pushed_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;

    TResourceInfo* _resource_info;

    int64_t _buffered_bytes;
    EvalConjunctsFn _eval_conjuncts_fn;

    bool _need_agg_finalize = true;

    // the max num of scan keys of this scan request.
    // it will set as BE's config `doris_max_scan_key_num`,
    // or be overwritten by value in TQueryOptions
    int32_t _max_scan_key_num = 1024;
    // The max number of conditions in InPredicate  that can be pushed down
    // into OlapEngine.
    // If conditions in InPredicate is larger than this, all conditions in
    // InPredicate will not be pushed to the OlapEngine.
    // it will set as BE's config `max_pushdown_conditions_per_column`,
    // or be overwritten by value in TQueryOptions
    int32_t _max_pushdown_conditions_per_column = 1024;

    struct RuntimeFilterContext {
        RuntimeFilterContext() : apply_mark(false), runtimefilter(nullptr) {}
        bool apply_mark;
        IRuntimeFilter* runtimefilter;
    };
    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::vector<RuntimeFilterContext> _runtime_filter_ctxs;
    std::map<int, RuntimeFilterContext*> _conjunctid_to_runtime_filter_ctxs;

    std::unique_ptr<RuntimeProfile> _scanner_profile;
    std::unique_ptr<RuntimeProfile> _segment_profile;

    // Counters
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompressor_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;

    RuntimeProfile::Counter* _rows_vec_cond_counter = nullptr;
    RuntimeProfile::Counter* _vec_cond_timer = nullptr;

    RuntimeProfile::Counter* _stats_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _del_filtered_counter = nullptr;
    RuntimeProfile::Counter* _conditions_filtered_counter = nullptr;
    RuntimeProfile::Counter* _key_range_filtered_counter = nullptr;

    RuntimeProfile::Counter* _block_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_seek_counter = nullptr;
    RuntimeProfile::Counter* _block_convert_timer = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    RuntimeProfile::Counter* _block_fetch_timer = nullptr;

    RuntimeProfile::Counter* _index_load_timer = nullptr;

    // total pages read
    // used by segment v2
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    // page read from cache
    // used by segment v2
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;

    // row count filtered by bitmap inverted index
    RuntimeProfile::Counter* _bitmap_index_filter_counter = nullptr;
    // time fro bitmap inverted index read and filter
    RuntimeProfile::Counter* _bitmap_index_filter_timer = nullptr;
    // number of created olap scanners
    RuntimeProfile::Counter* _num_scanners = nullptr;

    // number of segment filtered by column stat when creating seg iterator
    RuntimeProfile::Counter* _filtered_segment_counter = nullptr;
    // total number of segment related to this scan node
    RuntimeProfile::Counter* _total_segment_counter = nullptr;

    RuntimeProfile::Counter* _scanner_wait_batch_timer = nullptr;
    RuntimeProfile::Counter* _scanner_wait_worker_timer = nullptr;
};

} // namespace doris

#endif
