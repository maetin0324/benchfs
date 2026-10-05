# Locusta external user MR extension

`locusta-external-mr.patch` extends the embedded relay to accept externally
registered buffers (addr/lkey/rkey descriptor in the last 16 inline bytes),
adds `MemoryRegion::prefetch_odp` (ibv_advise_mr) and the experimental
`RoundtripGetHandle::reply_inline`. It is based on Locusta commit
`68f8b2104d33a98fe047e7e24f2394cebaee1c9c` (the parent gitlink before this
change) and does **not** apply to the older `0b5b83db` revision.

The same change is committed as Locusta
`31e8742726e7b4b1ac378f4ed8814e3f3b3fd2c9` (branch `benchfs-external-mr`,
parent `68f8b210`), and the `lib/locusta` gitlink of this BenchFS branch points
at that commit, so a normal checkout builds without applying the patch:

```sh
git submodule update --init lib/locusta   # needs the commit in the local store
cargo build --release --features mpi-support,transport-locusta --lib --bin benchfsd_mpi
```

On Sirius the commit is in `/work/NBB/rmaeda/workspace/rust/benchfs/lib/locusta/.git`
(ref `benchfs-external-mr`) and in the task worktree's module store. It has not
been pushed to the GitHub remote; until that is done, a fresh clone must either
fetch it from one of those stores or recreate it with the patch:

```sh
git -C lib/locusta checkout 68f8b2104d33a98fe047e7e24f2394cebaee1c9c
patch --dry-run -d lib/locusta -p1 < patches/locusta-external-mr.patch
patch -d lib/locusta -p1 < patches/locusta-external-mr.patch
```

Do not apply it twice. The standard arena descriptor remains unchanged. The
last 16 inline bytes hold an external descriptor only for the new request flag,
so its small header limit is 208 bytes. External buffers are rejected for the
standalone shared-memory relay, where client virtual addresses are not usable.
The measured binaries (jobs 43223 and the C experiment) were built from exactly
this tree (`68f8b210` + patch, byte-identical to commit `31e87427`).
