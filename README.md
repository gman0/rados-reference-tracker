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
