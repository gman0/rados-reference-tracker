#include "rt.h"
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>

#include <stdio.h>

/*

RT object layout
================

If not specified otherwise, all values are stored in big-endian order.

RT objects are versioned. Version is stored in object xattr as uint32_t.

Version 1:

    byte idx      type         name
    --------     ------       ------
     0 ..  3     uint32_t     refcount

    `refcount`: Number of references held by the RT object. The actual
                reference keys are stored in an OMap along with the RADOS
                object.

*/

// RT version xattr key.
#define RT_VERSION_XATTR "csi.ceph.com/rt-version"
// RT version type.
#define RT_VERSION_T uint32_t
// RT version size in bytes.
#define RT_VERSION_SIZE sizeof(RT_VERSION_T)
// Current RT object version.
#define RT_CURRENT_VERSION 1

// RT reference count type (Version 1).
#define RT_V1_REFCOUNT_T uint32_t
// RT reference count size (Version 1).
#define RT_V1_REFCOUNT_SIZE sizeof(RT_V1_REFCOUNT_T)

// Read RT object version from xattrs.
int read_rt_version(rados_ioctx_t ioctx, const char *oid, uint32_t *version);

// Initialize RT object (Version 1).
int init_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
            int keys_count);
// Add keys to RT object (Version 1).
int add_v1(rados_ioctx_t ioctx, const char *oid, uint64_t gen,
           const char *const *keys, int keys_count);
// Remove keys from RT object (Version 1).
int remove_v1(rados_ioctx_t ioctx, const char *oid, uint64_t gen,
              const char *const *keys, int keys_count, int *rt_removed);
// Read RT object (Version 1).
int read_v1(rados_ioctx_t ioctx, const char *oid, uint64_t gen,
            const char *const *keys, int keys_count, RT_V1_REFCOUNT_T *refcount,
            int *ref_keys_found);

/**
 * rt_add atomically adds keys to reference tracker.
 */
int rt_add(rados_t rados, const char *pool_name, const char *rt_name,
           const char *const *keys, int keys_count, int *rt_created) {
  { // Debug log message.
    printf("rt_add(): Adding %d keys:", keys_count);
    for (int i = 0; i < keys_count; i++)
      printf(" %s", keys[i]);
    printf(".\n");
  }

  int ret = 0;
  int created = 0;
  rados_ioctx_t ioctx = NULL;

  if ((ret = rados_ioctx_create(rados, pool_name, &ioctx)) < 0) {
    goto out;
  }

  // Read RT object version.

  RT_VERSION_T version;

  if ((ret = read_rt_version(ioctx, rt_name, &version)) < 0) {
    if (ret == -ENOENT) {
      // This is new RT. Initialize it with `keys`.

      { // Debug log message.
        printf("Got ENOENT. This must be a new RT object. Initialize it with "
               "provided keys.\n");
      }

      ret = init_v1(ioctx, rt_name, keys, keys_count);
      created = 1;
    }

    goto out;
  }

  // Add keys to tracked references.

  { // Debug log message.
    printf("Got RT object version %d.\n", version);
  }

  uint64_t gen = rados_get_last_version(ioctx);

  { // Debug log message.
    printf("RADOS object generation %lu.\n", gen);
  }

  switch (version) {
  case 1:
    ret = add_v1(ioctx, rt_name, gen, keys, keys_count);
    break;
  default:
    // Unknown version.
    { // Debug log message.
      printf("This is not a known RT object version.\n");
    }
    ret = -1;
    break;
  }

out:

  if (ioctx) {
    rados_ioctx_destroy(ioctx);
  }

  *rt_created = created;

  return ret;
}

/**
 * rt_remove atomically removes keys from reference tracker.
 */
int rt_remove(rados_t rados, const char *pool_name, const char *rt_name,
              const char *const *keys, int keys_count, int *rt_deleted) {
  { // Debug log message.
    printf("rt_remove(): Removing %d keys:", keys_count);
    for (int i = 0; i < keys_count; i++)
      printf(" %s", keys[i]);
    printf(".\n");
  }

  int ret = 0;
  int deleted = 0;
  rados_ioctx_t ioctx;

  if ((ret = rados_ioctx_create(rados, pool_name, &ioctx)) < 0) {
    goto out;
  }

  // Read RT object version.

  RT_VERSION_T version;

  if ((ret = read_rt_version(ioctx, rt_name, &version)) < 0) {
    if (ret == -ENOENT) {
      // This RT doesn't exist. Assume it was already deleted.

      { // Debug log message.
        printf("Got ENOENT. We're assuming the object must have been already "
               "deleted.\n");
      }

      ret = 0;
      deleted = 1;
    }

    goto out;
  }

  // Remove keys from tracked references.

  { // Debug log message.
    printf("Got RT object version %d.\n", version);
  }

  uint64_t gen = rados_get_last_version(ioctx);

  { // Debug log message.
    printf("RADOS object version %lu.\n", gen);
  }

  switch (version) {
  case 1:
    ret = remove_v1(ioctx, rt_name, gen, keys, keys_count, &deleted);
    break;
  default:
    // Unknown version.
    { // Debug log message.
      printf("This is not a known RT object version.\n");
    }
    ret = -1;
    break;
  }

out:

  if (ioctx) {
    rados_ioctx_destroy(ioctx);
  }

  *rt_deleted = deleted;

  return ret;
}

int read_rt_version(rados_ioctx_t ioctx, const char *oid,
                    RT_VERSION_T *version) {
  { // Debug log message.
    printf("Reading RT version...\n");
  }

  char version_bytes[RT_VERSION_SIZE];

  int ret;
  if ((ret = rados_getxattr(ioctx, oid, RT_VERSION_XATTR, version_bytes,
                            RT_VERSION_SIZE)) < 0) {
    return ret;
  }

  memcpy(version, version_bytes, RT_VERSION_SIZE);
  *version = ntohl(*version);

  return 0;
}

int init_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
            int keys_count) {
  { // Debug log message.
    printf("init_v1(): Initializing new RT v1 object.\n");
  }

  const int write_buf_size = RT_V1_REFCOUNT_SIZE;
  char write_buf[write_buf_size];

  // Prepare version.

  char version_bytes[RT_VERSION_SIZE];

  {
    RT_VERSION_T version = htonl(RT_CURRENT_VERSION);
    memcpy(version_bytes, &version, RT_VERSION_SIZE);
  }

  // Prepare reference count.

  {
    RT_V1_REFCOUNT_T refcount = htonl(keys_count);
    memcpy(write_buf, &refcount, RT_V1_REFCOUNT_SIZE);
  }

  // Prepare OMap entries.

  char **vals = malloc(sizeof(void *) * keys_count);
  size_t *key_lens = malloc(sizeof(size_t) * keys_count);
  size_t *val_lens = malloc(sizeof(size_t) * keys_count);

  for (int i = 0; i < keys_count; i++) {
    key_lens[i] = strlen(keys[i]);
    vals[i] = NULL;
    val_lens[i] = 0;
  }

  // Perform write.

  rados_write_op_t write_op = rados_create_write_op();

  rados_write_op_create(write_op, LIBRADOS_CREATE_EXCLUSIVE, NULL);
  rados_write_op_setxattr(write_op, RT_VERSION_XATTR, version_bytes,
                          RT_VERSION_SIZE);
  rados_write_op_write_full(write_op, write_buf, write_buf_size);
  rados_write_op_omap_set2(write_op, keys, (const char *const *)vals, key_lens,
                           (const size_t *)val_lens, keys_count);

  int ret = rados_write_op_operate(write_op, ioctx, oid, NULL, 0);

  { // Debug log message.
    if (ret < 0) {
      printf("Write operation failed with error code %d.\n", ret);
    } else {
      printf("RT object successfully initialized.\n");
    }
  }

  rados_release_write_op(write_op);

  free(val_lens);
  free(vals);
  free(key_lens);

  return ret;
}

int add_v1(rados_ioctx_t ioctx, const char *oid, uint64_t gen,
           const char *const *keys, int keys_count) {
  { // Debug log message.
    printf("add_v1(): Adding keys to an existing RT v1 object.\n");
  }

  int ret = 0;
  RT_V1_REFCOUNT_T refcount;

  const int write_buf_size = RT_V1_REFCOUNT_SIZE;
  char write_buf[write_buf_size];

  // Return values from OMap comparisons.
  int *ref_keys_found = malloc(sizeof(int) * keys_count);

  // Read the RT object.
  if ((ret = read_v1(ioctx, oid, gen, keys, keys_count, &refcount,
                     ref_keys_found)) < 0) {
    goto out;
  }

  // Prepare keys to add.

  int keys_to_add_count = 0;
  for (int i = 0; i < keys_count; i++) {
    if (!ref_keys_found[i]) {
      keys_to_add_count++;
    }
  }

  char **keys_to_add = NULL;
  char **vals_to_add = NULL;
  size_t *keys_to_add_lens = NULL;
  size_t *vals_to_add_lens = NULL;

  if (!keys_to_add_count) {
    // Nothing to do.
    { // Debug log message.
      printf("No keys will be added. They are all already tracked.\n");
    }
    goto out;
  }

  keys_to_add = malloc(sizeof(void *) * keys_to_add_count);
  vals_to_add = malloc(sizeof(void *) * keys_to_add_count);
  keys_to_add_lens = malloc(sizeof(size_t) * keys_to_add_count);
  vals_to_add_lens = malloc(sizeof(size_t) * keys_to_add_count);

  { // Debug log message.
    printf("Adding %d keys out of %d requested:", keys_to_add_count,
           keys_count);
  }

  for (int i = 0, j = 0; i < keys_count; i++) {
    if (ref_keys_found[i]) {
      // Skip this key as it's already tracked.
      continue;
    }

    keys_to_add[j] = (char *)keys[i];
    vals_to_add[j] = NULL;
    keys_to_add_lens[j] = strlen(keys[i]);
    vals_to_add_lens[j] = 0;

    j++;
    { // Debug log message.
      printf(" %s", keys[i]);
    }
  }

  { // Debug log message.
    printf(".\n");
  }

  // Prepare new value refcount.

  refcount += (RT_V1_REFCOUNT_T)keys_to_add_count;

  {
    RT_V1_REFCOUNT_T refcount_n = htonl(refcount);
    memcpy(write_buf, &refcount_n, RT_V1_REFCOUNT_SIZE);
  }

  // Perform write.

  {
    rados_write_op_t write_op = rados_create_write_op();

    rados_write_op_assert_version(write_op, gen);
    rados_write_op_write_full(write_op, write_buf, write_buf_size);
    rados_write_op_omap_set2(write_op, (const char *const *)keys_to_add,
                             (const char *const *)vals_to_add, keys_to_add_lens,
                             vals_to_add_lens, keys_to_add_count);

    ret = rados_write_op_operate(write_op, ioctx, oid, NULL, 0);
    rados_release_write_op(write_op);

    if (ret < 0) {
      { // Debug log message.
        if (ret == -ERANGE) {
          printf("The RT object has changed since it was last read. Please try "
                 "again.\n");
        } else {
          printf("Write operation failed with error code %d.\n", ret);
        }
      }
      goto out;
    }
  }

  { // Debug log message.
    printf("RT object successfully updated.\n");
  }

out:

  free(ref_keys_found);
  free(keys_to_add);
  free(vals_to_add);
  free(keys_to_add_lens);
  free(vals_to_add_lens);

  return ret;
}

int remove_v1(rados_ioctx_t ioctx, const char *oid, uint64_t gen,
              const char *const *keys, int keys_count, int *rt_removed) {
  { // Debug log message.
    printf("remove_v1(): Removing keys from an existing RT v1 object.\n");
  }

  int removed = 0;
  int ret = 0;
  RT_V1_REFCOUNT_T refcount;

  const int write_buf_size = RT_V1_REFCOUNT_SIZE;
  char write_buf[write_buf_size];

  // Return values from OMap comparisons.
  int *ref_keys_found = malloc(sizeof(int) * keys_count);

  // Read the RT object.
  if ((ret = read_v1(ioctx, oid, gen, keys, keys_count, &refcount,
                     ref_keys_found)) < 0) {
    goto out;
  }

  // Prepare keys to remove.

  int keys_to_remove_count = 0;
  for (int i = 0; i < keys_count; i++) {
    if (ref_keys_found[i]) {
      keys_to_remove_count++;
    }
  }

  char **keys_to_remove = NULL;
  size_t *keys_to_remove_lens = NULL;

  if (!keys_to_remove_count) {
    // Nothing to do.
    { // Debug log message.
      printf("No keys will be removed because none of the keys requested for "
             "removal are present.\n");
    }
    goto out;
  }

  keys_to_remove = malloc(sizeof(void *) * keys_to_remove_count);
  keys_to_remove_lens = malloc(sizeof(size_t) * keys_to_remove_count);

  { // Debug log message.
    printf("Removing %d keys out of %d requested:", keys_to_remove_count,
           keys_count);
  }

  for (int i = 0, j = 0; i < keys_count; i++) {
    if (!ref_keys_found[i]) {
      // Skip this key as it's been already removed from RT.
      continue;
    }

    keys_to_remove[j] = (char *)keys[i];
    keys_to_remove_lens[j] = strlen(keys[i]);

    j++;
    { // Debug log message.
      printf(" %s", keys[i]);
    }
  }

  { // Debug log message.
    printf(".\n");
  }

  // Prepare new value for refcount.

  refcount -= (RT_V1_REFCOUNT_T)keys_to_remove_count;

  {
    RT_V1_REFCOUNT_T refcount_n = htonl(refcount);
    memcpy(write_buf, &refcount_n, RT_V1_REFCOUNT_SIZE);
  }

  // Perform write operation.

  {
    rados_write_op_t write_op = rados_create_write_op();
    rados_write_op_assert_version(write_op, gen);

    if (refcount == 0) {
      // This RT holds no references, delete it.

      { // Debug log message.
        printf("After this operation, this RT would hold no references. "
               "Deleting the whole object instead.\n");
      }

      rados_write_op_remove(write_op);
      removed = 1;
    } else {
      // Update it with new values.

      rados_write_op_write_full(write_op, write_buf, write_buf_size);
      rados_write_op_omap_rm_keys2(write_op,
                                   (const char *const *)keys_to_remove,
                                   keys_to_remove_lens, keys_to_remove_count);
    }

    ret = rados_write_op_operate(write_op, ioctx, oid, NULL, 0);
    rados_release_write_op(write_op);

    if (ret < 0) {
      { // Debug log message.
        if (ret == -ERANGE) {
          printf("The RT object has changed since it was last read. Please try "
                 "again.\n");
        } else {
          printf("Write operation failed with error code %d.\n", ret);
        }
      }
    }
  }

  { // Debug log message.
    printf("RT object successfully updated.\n");
  }

out:

  free(ref_keys_found);
  free(keys_to_remove);
  free(keys_to_remove_lens);

  *rt_removed = removed;

  return ret;
}

int read_v1(rados_ioctx_t ioctx, const char *oid, uint64_t gen,
            const char *const *keys, int keys_count, RT_V1_REFCOUNT_T *refcount,
            int *ref_keys_found) {
  { // Debug log message.
    printf("read_v1(): Reading RT v1 object.\n");
  }

  int ret = 0;

  const int buf_size = RT_V1_REFCOUNT_SIZE;
  char read_buf[buf_size];

  // Here will be stored results of op_read.
  int read_rval;
  size_t read_bytes;

  // Prepare input for rados_read_op_omap_get_vals_by_keys2.

  size_t *key_lens = malloc(sizeof(size_t) * keys_count);
  for (int i = 0; i < keys_count; i++) {
    key_lens[i] = strlen(keys[i]);
  }

  const char **fetched_keys = NULL;

  rados_omap_iter_t omap_iter = NULL;
  int omap_get_vals_ret;

  // Perform read operation.

  {
    rados_read_op_t read_op = rados_create_read_op();

    rados_read_op_assert_version(read_op, gen);
    rados_read_op_read(read_op, 0, buf_size, read_buf, &read_bytes, &read_rval);
    rados_read_op_omap_get_vals_by_keys2(read_op, keys, keys_count, key_lens,
                                         &omap_iter, &omap_get_vals_ret);

    ret = rados_read_op_operate(read_op, ioctx, oid, 0);
    rados_release_read_op(read_op);

    if (ret < 0) {
      // Bail out on any error.
      goto out;
    }
  }

  // Populate ref_keys_found array. This could be implemented a bit nicer
  // than O(m*n), but it doesn't really matter as this is just a PoC.

  {
    unsigned iter_elems = rados_omap_iter_size(omap_iter);
    fetched_keys = malloc(sizeof(void *) * iter_elems);

    { // Debug log message.
      printf("Based on requested ref keys, we were able to fetch %d of them "
             "from RT OMap:",
             iter_elems);
    }

    for (unsigned i = 0; i < iter_elems; i++) {
      char *key, *val;
      size_t key_len, val_len;
      ret = rados_omap_get_next2(omap_iter, &key, &val, &key_len, &val_len);
      if (ret < 0) {
        { // Debug log message.
          printf("\nrados_omap_get_next2() failed with error code %d\n", ret);
        }
        goto out;
      }

      fetched_keys[i] = key;
      { // Debug log message.
        printf(" %s", key);
      }
    }

    { // Debug log message.
      printf(".\n");
    }

    for (int i = 0; i < keys_count; i++) {
      int found = 0;

      for (unsigned j = 0; j < iter_elems; j++) {
        if (strcmp(keys[i], fetched_keys[j]) == 0) {
          found = 1;
          break;
        }
      }

      ref_keys_found[i] = found;
    }
  }

  // Output refcount value.

  memcpy(refcount, read_buf, RT_V1_REFCOUNT_SIZE);
  *refcount = ntohl(*refcount);

out:

  rados_omap_get_end(omap_iter);

  free(key_lens);
  free(fetched_keys);

  return ret;
}
