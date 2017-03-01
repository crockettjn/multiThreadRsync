## Synopsis
This script runs a multi-threaded self governing rsync.  It will start as many rsync processes as there are CPU cores in the system as long as specified disk drives don't get too busy.  If the drives get too busy it will kill off a thread and re-que that job for later.  Also, if a rsync process dies for whatever reason it will re-que the job for later.  If your target server supports snapshots you can configure it to snap after the sync to retain point in time copies.  This is just the core of a program I use to backup 5K servers worldwide.

## Motivation
Backup software is expensive and single threaded rsync was way too slow.  One of the servers I backup has over 2TB of data that needs to be backed up daily.  It used to take 30 hours+ to back it up.  With 24 concurrent rsyncs it finishes in 1-2 hours now. :)

## Installation
Just configure the settings in the backupInfo dictionary in the main method and execute the script.
