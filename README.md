# rados-reference-tracker

This is a draft implementation of RADOS reference tracker in C.
Similar implementation, in Go, will be used for [ceph-csi](https://github.com/ceph/ceph-csi)
to track snapshot references for the work-in-progress
[shallow volume](https://github.com/ceph/ceph-csi/pull/2148) feature.

## Dependencies

Needs librados-dev / librados-devel.

## Building

Build using the supplied Makefile:
```
make
```

Resulting executable may be found in `build/reference-tracker`.

## Usage

```
reference-tracker -i CLIENT ID -p POOL NAME -c CEPH CONFIG FILE [-r RT NAME] -k REF KEYS -o RT OPERATION
```

* `-i CLIENT ID`: cephx client ID.
* `-p POOL NAME`: Ceph pool name.
* `-c CEPH CONFIG FILE`: Ceph config file.
* `-r RT NAME`: Name of the RADOS object for this reference tracker. Defaults to `hello-reference-tracker` if none provided.
* `-k REF KEYS`: Comma-separated list of keys to be used in the RT operation.
* `-o RT OPERATION`: Accepted values are `add` and `rem`. Specifies what to do with provided keys. `add` adds them to tracked references, `rem` removes them.
* `-h`: Program usage.

Example:
```
$ ./build/reference-tracker -i admin -p hello_world_pool -c /etc/ceph/ceph.conf -k key1,key2,key3 -o add
$ ./build/reference-tracker -i admin -p hello_world_pool -c /etc/ceph/ceph.conf -k key3,key4,key5 -o rem
```
