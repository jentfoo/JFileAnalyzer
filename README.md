JFileAnalyzer
=============

A small program to analyze a file structure, looking for duplicate files, as well as other things which may want to be cleaned up (poorly named files, etc).

The problem takes in paths and crawls along those paths to produce a list of files.  Then in parallel it processes through that list of files.  The current implementation does two things, examine files names to produce a list of media files that should be renamed.  And does a hash of all the files to produce list of duplicated files (thus providing a list of things that can be deleted).

There are other applications that search for duplicated files using hashing, but most (if any) are not parallel.  Thus for examining large amounts of files, it allows you to continue to read data from the disk, while hashes are being created and compared in parallel (thus taking better usage of the CPU since this is almost always an IO bound operation).
