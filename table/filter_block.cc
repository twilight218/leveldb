// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase); // 对于所有datablock来说，每2K就要建一个bloom filter
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {  // 每次切datablock，都要生成更多filter
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size()); // 记录key在总字符串中的偏移量
  keys_.append(k.data(), k.size()); // 把key追加到总字符串中
}

// filter block只有一个，在最后记录每个filter的偏移和偏移列表的起始位置和基数，返回整个bloom filter
Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter(); // 最后1个datablock是finish()调用而不是startblock()，最后1个datablock的key全塞进1个Filter
  }

  // Append array of per-filter offsets
  // 记录每个bloom filter的开始偏移量
  const uint32_t array_offset = result_.size(); // 下面offset列表的起始位置
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset); // 最后记录bloom offset偏移量列表的起始位置
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    // 取出Add添加的每个key
    const char* base = keys_.data() + start_[i]; 
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());  // 记录新bloom过滤器在result_中的起始位置
  // 看bloom.cc
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);   // 添加下一个bloom过滤器到result_

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1]; 
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);  // offset列表的位置
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4; // 计算出有几个bloom filter
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;  //   其实就是block_offset / 2^base_lg_，找到当前datablock对应的filter是哪个
  if (index < num_) {
    // 根据filter offset和下一个filter offset，读取当前filter的bloom数据
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) { 
      Slice filter = Slice(data_ + start, limit - start); 
      return policy_->KeyMayMatch(key, filter); // 检查这个key在不在这个bloom filter里面（看bloom.cc）
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
