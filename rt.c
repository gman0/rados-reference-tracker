#include "rt.h"
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>

#include <stdio.h>

/*

RT object layout

If not specified otherwise, all values are stored in big-endian order.

RT objects are versioned. Version is stored in object xattr as uint32_t.

Version 1:

    byte idx      type         name
    --------     ------       ------
     0 ..  3     uint32_t     gen
     4 ..  7     uint32_t     refcount

    `gen`: Generation number. Incremented each time the RT object is modified.
           This is used as a comparison value in test-and-set procedures.
           The write transaction is aborted if `gen` has changed since the last
           time the object was read.

    `refcount`: Number of references held by the RT object. The actual
                reference keys are stored in an Omap along with the RADOS
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

// RT generation number type (Version 1).
#define RT_V1_GEN_T uint32_t
// RT generation number size (Version 1).
#define RT_V1_GEN_SIZE sizeof(RT_V1_GEN_T)
// RT reference count type (Version 1).
#define RT_V1_REFCOUNT_T uint32_t
// RT reference count size (Version 1).
#define RT_V1_REFCOUNT_SIZE sizeof(RT_V1_GEN_T)

// Read RT object version from xattrs.
int read_version(rados_ioctx_t ioctx, const char *oid, uint32_t *version);

// Initialize RT object (Version 1).
int init_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
            int keys_count);
// Add keys to RT object (Version 1).
int add_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
           int keys_count);
// Remove keys from RT object (Version 1).
int remove_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
              int keys_count, int *rt_removed);
// Read RT object (Version 1).
int read_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
            int keys_count, RT_V1_GEN_T *gen, RT_V1_REFCOUNT_T *refcount,
            int *ref_keys_found);

/**
 * rt_add atomically adds keys to reference tracker.
 */
int rt_add(rados_t rados, const char *pool_name, const char *rt_name,
           const char *const *keys, int keys_count, int *rt_created) {
  {
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

  if ((ret = read_version(ioctx, rt_name, &version)) < 0) {
    if (ret == -ENOENT) {
      // This is new RT. Initialize it with `keys`.

      {
        printf("Got ENOENT. This must be a new RT object. Initialize it with "
               "provided keys.\n");
      }

      ret = init_v1(ioctx, rt_name, keys, keys_count);
      created = 1;
    }

    goto out;
  }

  // Add keys to tracked references.

  { printf("Got RT object version %d.\n", version); }

  switch (version) {
  case 1:
    ret = add_v1(ioctx, rt_name, keys, keys_count);
    break;
  default:
    // Unknown version.
    { printf("This is not a known RT object version.\n"); }
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
  {
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

  if ((ret = read_version(ioctx, rt_name, &version)) < 0) {
    if (ret == -ENOENT) {
      // This RT doesn't exist. Assume it was already deleted.

      {
        printf("Got ENOENT. We're assuming the object must have been already "
               "deleted.\n");
      }

      ret = 0;
      deleted = 1;
    }

    goto out;
  }

  // Remove keys from tracked references.

  { printf("Got RT object version %d.\n", version); }

  switch (version) {
  case 1:
    ret = remove_v1(ioctx, rt_name, keys, keys_count, &deleted);
    break;
  default:
    // Unknown version.
    { printf("This is not a known RT object version.\n"); }
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

int read_version(rados_ioctx_t ioctx, const char *oid, RT_VERSION_T *version) {
  { printf("Reading RT version...\n"); }

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
  { printf("init_v1(): Initializing new RT v1 object.\n"); }

  // Prepare version.

  char version_bytes[RT_VERSION_SIZE];

  {
    RT_VERSION_T version = htonl(RT_CURRENT_VERSION);
    memcpy(version_bytes, &version, RT_VERSION_SIZE);
  }

  // Common buffer for gen and refcount so that it fits
  // in a single write_full step.
  const int write_buf_size = RT_V1_GEN_SIZE + RT_V1_REFCOUNT_SIZE;
  char write_buf[write_buf_size];

  // Prepare generation number.

  {
    RT_V1_GEN_T gen = htonl(1);
    memcpy(write_buf, &gen, RT_V1_GEN_SIZE);
  }

  // Prepare reference count.

  {
    RT_V1_REFCOUNT_T refcount = htonl(keys_count);
    memcpy(write_buf + RT_V1_GEN_SIZE, &refcount, RT_V1_REFCOUNT_SIZE);
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

  {
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

int add_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
           int keys_count) {
  { printf("add_v1(): Adding keys to an existing RT v1 object.\n"); }

  int ret = 0;
  RT_V1_GEN_T gen;
  RT_V1_REFCOUNT_T refcount;

  // Common buffer for gen and refcount so that it fits
  // in a single write_full step.
  const int write_buf_size = RT_V1_GEN_SIZE + RT_V1_REFCOUNT_SIZE;
  char write_buf[write_buf_size];

  // Buffer for gen comparison.
  char gen_cmp_buf[RT_V1_GEN_SIZE];

  // Return values from OMap comparisons.
  int *ref_keys_found = malloc(sizeof(int) * keys_count);

  // Read the RT object.
  if ((ret = read_v1(ioctx, oid, keys, keys_count, &gen, &refcount,
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
    { printf("No keys will be added. They are all already tracked.\n"); }
    goto out;
  }

  keys_to_add = malloc(sizeof(void *) * keys_to_add_count);
  vals_to_add = malloc(sizeof(void *) * keys_to_add_count);
  keys_to_add_lens = malloc(sizeof(size_t) * keys_to_add_count);
  vals_to_add_lens = malloc(sizeof(size_t) * keys_to_add_count);

  {
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
    { printf(" %s", keys[i]); }
  }

  { printf(".\n"); }

  // Prepare buffer for gen comparison.

  {
    RT_V1_GEN_T gen_n = htonl(gen);
    memcpy(gen_cmp_buf, &gen_n, RT_V1_GEN_SIZE);
  }

  // Prepare new values for gen and refcount.

  gen++;
  refcount += (RT_V1_REFCOUNT_T)keys_to_add_count;

  {
    RT_V1_GEN_T gen_n = htonl(gen);
    RT_V1_REFCOUNT_T refcount_n = htonl(refcount);

    memcpy(write_buf, &gen_n, RT_V1_GEN_SIZE);
    memcpy(write_buf + RT_V1_GEN_SIZE, &refcount_n, RT_V1_REFCOUNT_SIZE);
  }

  // Perform write.

  {
    rados_write_op_t write_op = rados_create_write_op();

    int gen_cmp_rval;
    rados_write_op_cmpext(write_op, gen_cmp_buf, RT_V1_GEN_SIZE, 0,
                          &gen_cmp_rval);
    rados_write_op_write_full(write_op, write_buf, write_buf_size);
    rados_write_op_omap_set2(write_op, (const char *const *)keys_to_add,
                             (const char *const *)vals_to_add, keys_to_add_lens,
                             vals_to_add_lens, keys_to_add_count);

    ret = rados_write_op_operate(write_op, ioctx, oid, NULL, 0);
    rados_release_write_op(write_op);

    if (ret < 0) {
      {
        if (gen_cmp_rval != 0) {
          printf("The RT object has changed since it was last read. Please try "
                 "again.\n");
        } else {
          printf("Write operation failed with error code %d.\n", ret);
        }
      }
      goto out;
    }
  }

  { printf("RT object successfully updated.\n"); }

out:

  free(ref_keys_found);
  free(keys_to_add);
  free(vals_to_add);
  free(keys_to_add_lens);
  free(vals_to_add_lens);

  return ret;
}

int remove_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
              int keys_count, int *rt_removed) {
  { printf("remove_v1(): Removing keys from an existing RT v1 object.\n"); }

  int removed = 0;
  int ret = 0;
  RT_V1_GEN_T gen;
  RT_V1_REFCOUNT_T refcount;

  // Common buffer for gen and refcount so that it fits
  // in a single write_full step.
  const int write_buf_size = RT_V1_GEN_SIZE + RT_V1_REFCOUNT_SIZE;
  char write_buf[write_buf_size];

  // Buffer for gen comparison.
  char gen_cmp_buf[RT_V1_GEN_SIZE];

  // Return values from OMap comparisons.
  int *ref_keys_found = malloc(sizeof(int) * keys_count);

  // Read the RT object.
  if ((ret = read_v1(ioctx, oid, keys, keys_count, &gen, &refcount,
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
    {
      printf("No keys will be removed because none of the keys requested for "
             "removal are present.\n");
    }
    goto out;
  }

  keys_to_remove = malloc(sizeof(void *) * keys_to_remove_count);
  keys_to_remove_lens = malloc(sizeof(size_t) * keys_to_remove_count);

  {
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
    { printf(" %s", keys[i]); }
  }

  { printf(".\n"); }

  // Prepare buffer for gen comparison.

  {
    RT_V1_GEN_T gen_n = htonl(gen);
    memcpy(gen_cmp_buf, &gen_n, RT_V1_GEN_SIZE);
  }

  // Prepare new values for gen and refcount.

  gen++;
  refcount -= (RT_V1_REFCOUNT_T)keys_to_remove_count;

  {
    RT_V1_GEN_T gen_n = htonl(gen);
    RT_V1_REFCOUNT_T refcount_n = htonl(refcount);

    memcpy(write_buf, &gen_n, RT_V1_GEN_SIZE);
    memcpy(write_buf + RT_V1_GEN_SIZE, &refcount_n, RT_V1_REFCOUNT_SIZE);
  }

  // Perform write operation.

  {
    rados_write_op_t write_op = rados_create_write_op();

    int gen_cmp_rval;
    rados_write_op_cmpext(write_op, gen_cmp_buf, RT_V1_GEN_SIZE, 0,
                          &gen_cmp_rval);

    if (refcount == 0) {
      // This RT holds no references, delete it.

      {
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
      {
        if (gen_cmp_rval != 0) {
          printf("The RT object has changed since it was last read. Please try "
                 "again.\n");
        } else {
          printf("Write operation failed with error code %d.\n", ret);
        }
      }
    }
  }

  { printf("RT object successfully updated.\n"); }

out:

  free(ref_keys_found);
  free(keys_to_remove);
  free(keys_to_remove_lens);

  *rt_removed = removed;

  return ret;
}

int read_v1(rados_ioctx_t ioctx, const char *oid, const char *const *keys,
            int keys_count, RT_V1_GEN_T *gen, RT_V1_REFCOUNT_T *refcount,
            int *ref_keys_found) {
  { printf("read_v1(): Reading RT v1 object.\n"); }

  int ret = 0;

  // Common buffer for gen and refcount so that both of these values can be
  // fetched by a single read step.
  const int buf_size = RT_V1_GEN_SIZE + RT_V1_REFCOUNT_SIZE;
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

    {
      printf("Based on requested ref keys, we were able to fetch %d of them "
             "from RT OMap:",
             iter_elems);
    }

    for (unsigned i = 0; i < iter_elems; i++) {
      char *key, *val;
      size_t key_len, val_len;
      ret = rados_omap_get_next2(omap_iter, &key, &val, &key_len, &val_len);
      if (ret < 0) {
        { printf("\nrados_omap_get_next2() failed with error code %d\n", ret); }
        goto out;
      }

      fetched_keys[i] = key;
      { printf(" %s", key); }
    }

    { printf(".\n"); }

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

  // Output gen and refcount values.

  memcpy(gen, read_buf, RT_V1_GEN_SIZE);
  memcpy(refcount, read_buf + RT_V1_GEN_SIZE, RT_V1_REFCOUNT_SIZE);
  *gen = ntohl(*gen);
  *refcount = ntohl(*refcount);

  {
    printf("The RT object tracks %d references in total. It's been updated %d "
           "times so far.\n",
           *refcount, *gen);
  }

out:

  rados_omap_get_end(omap_iter);

  free(key_lens);
  free(fetched_keys);

  return ret;
}
