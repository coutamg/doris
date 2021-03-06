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

#include "olap/file_stream.h"

#include "olap/byte_buffer.h"
#include "olap/out_stream.h"

namespace doris {

ReadOnlyFileStream::ReadOnlyFileStream(FileHandler* handler, StorageByteBuffer** shared_buffer,
                                       Decompressor decompressor, uint32_t compress_buffer_size,
                                       OlapReaderStatistics* stats)
        : _file_cursor(handler, 0, 0),
          _compressed_helper(NULL),
          _uncompressed(NULL),
          _shared_buffer(shared_buffer),
          _decompressor(decompressor),
          _compress_buffer_size(compress_buffer_size + sizeof(StreamHead)),
          _current_compress_position(std::numeric_limits<uint64_t>::max()),
          _stats(stats) {}

ReadOnlyFileStream::ReadOnlyFileStream(FileHandler* handler, StorageByteBuffer** shared_buffer,
                                       uint64_t offset, uint64_t length, Decompressor decompressor,
                                       uint32_t compress_buffer_size, OlapReaderStatistics* stats)
        : _file_cursor(handler, offset, length),
          _compressed_helper(NULL),
          _uncompressed(NULL),
          _shared_buffer(shared_buffer),
          _decompressor(decompressor),
          _compress_buffer_size(compress_buffer_size + sizeof(StreamHead)),
          _current_compress_position(std::numeric_limits<uint64_t>::max()),
          _stats(stats) {}

OLAPStatus ReadOnlyFileStream::_assure_data() {
    // if still has data in uncompressed
    if (OLAP_LIKELY(_uncompressed != NULL && _uncompressed->remaining() > 0)) {
        return OLAP_SUCCESS;
    } else if (_file_cursor.eof()) {
        VLOG_TRACE << "STREAM EOF. length=" << _file_cursor.length()
                 << ", used=" << _file_cursor.position();
        return OLAP_ERR_COLUMN_STREAM_EOF;
    }

    StreamHead header;
    size_t file_cursor_used = _file_cursor.position();
    OLAPStatus res = OLAP_SUCCESS;
    {
        SCOPED_RAW_TIMER(&_stats->io_ns);
        res = _file_cursor.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (OLAP_UNLIKELY(OLAP_SUCCESS != res)) {
            OLAP_LOG_WARNING("read header fail");
            return res;
        }
        res = _fill_compressed(header.length);
        if (OLAP_UNLIKELY(OLAP_SUCCESS != res)) {
            OLAP_LOG_WARNING("read header fail");
            return res;
        }
        _stats->compressed_bytes_read += sizeof(header) + header.length;
    }

    if (header.type == StreamHead::UNCOMPRESSED) {
        StorageByteBuffer* tmp = _compressed_helper;
        _compressed_helper = *_shared_buffer;
        *_shared_buffer = tmp;
    } else {
        _compressed_helper->set_position(0);
        _compressed_helper->set_limit(_compress_buffer_size);
        {
            SCOPED_RAW_TIMER(&_stats->decompress_ns);
            res = _decompressor(*_shared_buffer, _compressed_helper);
            if (OLAP_SUCCESS != res) {
                OLAP_LOG_WARNING("fail to decompress err=%d", res);
                return res;
            }
        }
    }
    _stats->uncompressed_bytes_read += _compressed_helper->limit();

    _uncompressed = _compressed_helper;
    _current_compress_position = file_cursor_used;
    return res;
}

// ?????????????????????
OLAPStatus ReadOnlyFileStream::seek(PositionProvider* position) {
    OLAPStatus res = OLAP_SUCCESS;
    // ???seek?????????????????????????????????writer????????????spilled byte
    int64_t compressed_position = position->get_next();
    int64_t uncompressed_bytes = position->get_next();
    if (_current_compress_position == compressed_position && NULL != _uncompressed) {
        /*
         * ???????????????????????????_uncompressed???NULL????????????
         * ???varchar?????????????????????????????????????????????_uncompressed == NULL ???
         * ????????????????????????A????????????????????????, ??????????????????
         * ????????????varchar????????????????????????_uncompressed == NULL???
         * ???????????????segmentreader????????????????????????A?????????????????????????????????
         */
    } else {
        _file_cursor.seek(compressed_position);
        _uncompressed = NULL;

        res = _assure_data();
        if (OLAP_LIKELY(OLAP_SUCCESS == res)) {
            // assure data will be successful in most case
        } else if (res == OLAP_ERR_COLUMN_STREAM_EOF) {
            VLOG_TRACE << "file stream eof.";
            return res;
        } else {
            OLAP_LOG_WARNING("fail to assure data after seek");
            return res;
        }
    }

    res = _uncompressed->set_position(uncompressed_bytes);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to set position.[res=%d, position=%lu]", res, uncompressed_bytes);
        return res;
    }

    return OLAP_SUCCESS;
}

// ???????????????size??????
OLAPStatus ReadOnlyFileStream::skip(uint64_t skip_length) {
    OLAPStatus res = _assure_data();

    if (OLAP_SUCCESS != res) {
        return res;
    }

    uint64_t skip_byte = 0;
    uint64_t byte_to_skip = skip_length;

    // ???????????????????????????????????????????????????????????????????????????
    // ?????????????????????????????? ????????????EOF
    do {
        skip_byte = std::min(_uncompressed->remaining(), byte_to_skip);
        _uncompressed->set_position(_uncompressed->position() + skip_byte);
        byte_to_skip -= skip_byte;
        // ???????????????????????????????????????assure????????????????????????
        // ??????????????????????????????skip_length,??????_assure_data??????????????????
        res = _assure_data();
        // while????????????????????????????????????
    } while (byte_to_skip != 0 && res == OLAP_SUCCESS);

    return res;
}

OLAPStatus ReadOnlyFileStream::_fill_compressed(size_t length) {
    if (length > _compress_buffer_size) {
        LOG(WARNING) << "overflow when fill compressed."
                     << ", length=" << length << ", compress_size" << _compress_buffer_size;
        return OLAP_ERR_OUT_OF_BOUND;
    }

    OLAPStatus res = _file_cursor.read((*_shared_buffer)->array(), length);
    if (OLAP_SUCCESS != res) {
        OLAP_LOG_WARNING("fail to fill compressed buffer.");
        return res;
    }

    (*_shared_buffer)->set_position(0);
    (*_shared_buffer)->set_limit(length);
    return res;
}

uint64_t ReadOnlyFileStream::available() {
    return _file_cursor.remain();
}

} // namespace doris
