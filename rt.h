#ifndef rt_h_INCLUDED
#define rt_h_INCLUDED

#include <rados/librados.h>

/**
 * Reference tracker, RT, is a reference counter, but instead of being
 * integer-based, it counts references based on supplied keys, identifying the
 * references. This allows accounting in situations where idempotency must be
 * preserved. It guarantees there will be no duplicate increments/decrements of
 * the counter.
 *
 * It is safe to use with multiple concurrent writers, and across different
 * nodes of a cluster.
 */

/**
 * rt_add atomically adds keys to reference tracker.
 *
 * `rados` is a handle to a Ceph cluster.
 * `pool_name` is name of the pool where the RT RADOS object is stored.
 * `rt_name` is name of the reference tracker RADOS object.
 * `keys` is an array of key strings to add to RT. If a particular key
 *        already exists in the RT, it won't be added again and is
 *        considered as a success.
 * `keys_count` is the number of keys in `keys` array.
 * `rt_created` is set to non-zero value in case the reference tracker
 *              was created by this call.
 */
int rt_add(rados_t rados, const char *pool_name, const char *rt_name,
           const char *const *keys, int keys_count, int *rt_created);

/**
 * rt_remove atomically removes keys from reference tracker.
 *
 * `rados` is a handle to a Ceph cluster.
 * `pool_name` is name of the pool where the RT RADOS object is stored.
 * `rt_name` is name of the reference tracker RADOS object.
 * `keys` is an array of key strings to remove from RT. If a particular
 *        key already doesn't exist the RT, it's assumed the key was
 *        already deleted -- this is considered as a success.
 * `keys_count` is the number of keys in `keys` array.
 * `rt_deleted` is set to non-zero value in case the reference tracker
 *              holds no references anymore and the RT RADOS object has
 *              been deleted.
 */
int rt_remove(rados_t rados, const char *pool_name, const char *rt_name,
              const char *const *keys, int keys_count, int *rt_deleted);

#endif // rt_h_INCLUDED
