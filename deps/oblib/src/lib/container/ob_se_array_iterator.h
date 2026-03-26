/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _OB_SE_ARRAY_ITERATOR_H
#define _OB_SE_ARRAY_ITERATOR_H 1
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace common
{
template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
typename ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::iterator ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::begin()
{
  return iterator(data_);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
typename ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::iterator ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::end()
{
  return iterator(data_ + count_);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
typename ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::const_iterator
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::begin() const
{
  return const_iterator(data_);
}

template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT, bool auto_free>
typename ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::const_iterator
ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::end() const
{
  return const_iterator(data_ + count_);
}

namespace array
{
template <typename T, int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT,
          bool auto_free>
class ObSEArrayIterator
{
  friend class ObSEArrayImpl<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>;
public:
  typedef T value_type;
  typedef int64_t difference_type;
  typedef T *value_ptr_t;
  typedef T *pointer;
  typedef T &reference;
  typedef std::random_access_iterator_tag iterator_category;

public:
  ObSEArrayIterator() : value_ptr_(NULL) {}
  explicit ObSEArrayIterator(value_ptr_t value_ptr) : value_ptr_(value_ptr) {}
  inline reference operator*() const
  {
    return *value_ptr_;
  }
  inline value_ptr_t operator->() const
  {
    return value_ptr_;
  }

  operator value_ptr_t() const { return value_ptr_; }

  inline ObSEArrayIterator operator++(int) // ObSEArrayIterator++
  {
    return ObSEArrayIterator(value_ptr_++);
  }
  inline ObSEArrayIterator &operator++()
  {
    value_ptr_++;
    return *this;
  }
  inline ObSEArrayIterator operator--(int)
  {
    return ObSEArrayIterator(value_ptr_--);
  }
  inline ObSEArrayIterator &operator--()
  {
    value_ptr_--;
    return *this;
  }
  inline ObSEArrayIterator operator+(int64_t off) const
  {
    return ObSEArrayIterator(value_ptr_ + off);
  }
  inline ObSEArrayIterator &operator+=(int64_t off)
  {
    value_ptr_ += off;
    return *this;
  }
  inline difference_type operator-(const ObSEArrayIterator &rhs) const
  {
    return value_ptr_ - rhs.value_ptr_;
  }
  inline ObSEArrayIterator operator-(int64_t off) const
  {
    return ObSEArrayIterator(value_ptr_ - off);
  }
  inline ObSEArrayIterator &operator-=(int64_t off)
  {
    value_ptr_ -= off;
    return *this;
  }
  inline bool operator==(const ObSEArrayIterator &rhs) const
  {
    return (value_ptr_ == rhs.value_ptr_);
  }
  inline bool operator==(const long &value) const
  {
    UNUSED(value);
    return (value_ptr_ == NULL);
  }
  inline bool operator!=(const ObSEArrayIterator &rhs) const
  {
    return (value_ptr_ != rhs.value_ptr_);
  }
  inline bool operator<(const ObSEArrayIterator &rhs) const
  {
    return (value_ptr_ < rhs.value_ptr_);
  }

  inline bool operator<=(const ObSEArrayIterator &rhs) const
  {
    return (value_ptr_ <= rhs.value_ptr_);
  }
private:
  value_ptr_t value_ptr_;
};

template <typename T, int64_t LOCAL_ARRAY_SIZE,
          typename BlockAllocatorT,
          bool auto_free>
ObSEArrayIterator<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> operator+(
  typename ObSEArrayIterator<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>::difference_type diff,
  const ObSEArrayIterator<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free>& iter)
{
  ObSEArrayIterator<T, LOCAL_ARRAY_SIZE, BlockAllocatorT, auto_free> iter2 = iter;
  iter2 += diff;
  return iter2;
}

}
}
}

#endif /* _OB_SE_ARRAY_ITERATOR_H */



