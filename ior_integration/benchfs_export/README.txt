BenchFS IOR Modifications
=========================

This archive contains all BenchFS-specific modifications for IOR.

Contents:
---------
1. benchfs_ior.patch       - Git patch for IOR source modifications
2. benchfs_backend.tar.gz  - BenchFS backend implementation files
3. apply_modifications.sh  - Automated application script

Installation on Supercomputer:
-------------------------------

1. Transfer this directory to the supercomputer:

   scp -r benchfs_export/ <supercomputer>:/path/to/benchfs/ior_integration/

2. On the supercomputer, navigate to the IOR integration directory:

   cd /path/to/benchfs/ior_integration

3. Clone IOR repository (if not already done):

   git clone https://github.com/hpc/ior.git
   cd ior

4. Apply BenchFS modifications:

   cd ../benchfs_export
   ./apply_modifications.sh

5. Build IOR with BenchFS support:

   cd ../ior
   ./bootstrap
   ./configure
   make -j$(nproc)

6. Verify BenchFS backend is available:

   ./src/ior -h | grep BENCHFS

Manual Installation:
--------------------

If the automated script fails, you can apply modifications manually:

1. Extract benchfs_backend:

   cd /path/to/benchfs/ior_integration
   tar xzf benchfs_export/benchfs_backend.tar.gz

2. Copy BenchFS backend to IOR:

   cp benchfs_backend/src/aiori-BENCHFS.c ior/src/

3. Apply git patch:

   cd ior
   git apply ../benchfs_export/benchfs_ior.patch

4. Build IOR:

   ./bootstrap
   ./configure
   make -j$(nproc)

Troubleshooting:
----------------

If patch application fails:
  - The IOR repository might be at a different version
  - Apply changes manually by editing the files listed in the patch

If configure fails:
  - Ensure MPI is loaded: module load openmpi
  - Check that autotools are available: module load autotools

If build fails:
  - Check that benchfs_c_api.h is in the include path
  - Ensure BenchFS Rust library is built

For more information, see:
  - ior_integration/README.md
  - SETUP_IOR.md

