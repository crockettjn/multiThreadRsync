#!/usr/bin/python
import operator
import os
import sys
from Queue import Queue
import threading
from threading import Thread
import time
import subprocess
import multiprocessing
import signal
import linux_metrics as lm
import psutil


class ActivePool(object):
    def __init__(self):
        super(ActivePool, self).__init__()
        self.active = []
        self.lock = threading.Lock()

    def makeActive(self, name):
        with self.lock:
            self.active.append(name)
            print("Making {} active.".format(name))

    def makeInactive(self, name):
        with self.lock:
            try:
                self.active.remove(name)
                print("Making {} inactive.".format(name))
            except:
                print("{} not in the list.".format(name))

    def numActive(self):
        with self.lock:
            return len(self.active)


class rsyncer:
    def __init__(self, backupInfo):
        self.backupInfo = backupInfo
        self.que = Queue(maxsize=0)
        self.backupList = self.enumFiles()
        self.tpool = ActivePool()
        self.totalSize = 0
        self.completedJobs = 0
        self.killThread = False

    def enumFiles(self):
        d = {}
        folders = self.backupInfo['folders'].split(',')
        for dcty in folders:
            for root, dirs, files in os.walk(dcty):
                for f in files:
                    fl = root + '/' + f
                    size = os.path.getsize(root + os.sep + f)
                    d[fl] = size
        sorted_d = sorted(d.items(), key=operator.itemgetter(1), reverse=True)
        print("Found " + str(len(sorted_d)) + " files.")
        return sorted_d

    def runRsync(self):
        flags = self.backupInfo['rsyncFlags']
        target = self.backupInfo['target']
        targetPath = self.backupInfo['targetPath']
        source = self.backupInfo['source']
        go = True
        while True:
            if go:
                name = threading.current_thread().name
                if self.que.qsize() != 0:
                    self.tpool.makeActive(name)
                    fileName = '/tmp/threadList_' + name
                    f = open(fileName, 'w', 0)
                    files = self.que.get()
                    for ent in files:
                        f.write(ent + '\n')
                    f.close
                    cmd = ("rsync {} -e \"ssh \" --files-from={} / {}:{}{}/").format(flags, fileName, target, targetPath, source)
                    print(cmd)
                    print('Starting Rsync for thread ' + name)
                    process = subprocess.Popen(cmd, shell=True)
                    process.communicate()
                    print("Return Code: " + str(process.returncode))
                    if process.returncode != 0:
                        print("rsync failed, re-queing task.")
                        if self.killThread:
                            self.tpool.makeInactive(name)
                            self.killThread = False
                            go = False
                        self.queAdd(files)
                    else:
                        print('Rsync for thread {} finished.'.format(name))
                        self.completedJobs += 1
                    self.tpool.makeInactive(name)
                    os.remove(fileName)
                    self.que.task_done()
                else:
                    go = False
            else:
                break

    def queAdd(self, item):
        self.que.put(item)

    def queLoad(self):
        chunkbytes = int(self.backupInfo['chunkSizeGB'] * 1024 * 1024 * 1024)
        fileList = []
        filesize = 0
        for x in self.backupList:
            filesize += x[1]
            fileList.append(x[0])
            if filesize > chunkbytes:
                self.queAdd(fileList)
                fileList = []
                self.totalSize += filesize
                filesize = 0
        self.queAdd(fileList)

    def waitForQueue(self):
        print("Started waiting for threads to complete")
        self.que.join()

    def threadWatcher(self):
        time.sleep(5)
        driveLow = self.backupInfo['driveBusyMin']
        driveHigh = self.backupInfo['driveBusyMax']
        num_cpus = multiprocessing.cpu_count()
        queueSize = self.que.qsize()
        threads = self.tpool.numActive()
        load = lm.load_avg()[1]
        diskBusy = self.getDiskBusy()
        print("Queue size: " + str(queueSize))
        print("Load: " + str(load))
        print("Drive Busy percent: " + str(diskBusy))
        print("Active Threads: " + str(threads))
        print("Total sizeGB: " + str(self.totalSize / 1024 / 1024 / 1024) +
              " Completed jobs: " + str(self.completedJobs))

        if threads < num_cpus and load < num_cpus and queueSize > 0 and diskBusy < driveLow:
            print("Creating additional thread.")
            self.spawnThread()
        elif threads >= num_cpus:
            print("Number of threads equal number of CPUs, limit reached.")
        elif queueSize < 1:
            print("The queue is empty, new threads not needed.")
        elif diskBusy >= driveLow:
            print("Drives higher than minimum, no new threads.")

        if threads > num_cpus or load > num_cpus or diskBusy > driveHigh:
            if threads > num_cpus:
                print("Too many threads, killing one.")
            elif load > num_cpus:
                print("Load too high, killing a thread.")
            elif diskBusy > driveHigh:
                print("The drives are too busy, killing a thread.")
            self.killThread = True
            self.subprocessKiller()

    def subprocessKiller(self):
        pprocess = psutil.Process()
        children = pprocess.children(recursive=True)
        try:
            # Kill the newest rsync process rather than the oldest.
            os.kill(children[-1].pid, signal.SIGTERM)
        except Exception as e:
            print(e)

    def spawnThread(self):
        print("Starting New Thread.")
        worker = Thread(target=self.runRsync)
        worker.setDaemon(True)
        worker.start()

    def getDiskBusy(self):
        # Note, for every drive this function pauses for the sample_duration
        drives = []
        devices = self.backupInfo['drives'].split(',')
        for d in devices:
            try:
                drives.append(lm.disk_busy(d, sample_duration=10))
            except Exception as e:
                print(e)
        diskBusy = sum(drives) / float(len(drives))
        return diskBusy


def main():
    backupInfo = {
        'source': 'mybox.domain',  # Source server
        'target': 'mytargetserver.domain',  # Target server
        'targetPath': '/home/backups/',  # Path on target server to rsync backups to
        'driveBusyMin': 50,  # Rsync threads will continue to be created until this number is reached
        'driveBusyMax': 80,  # Rsync threads will be killed if disk busy gets over this number
        'chunkSizeGB': 5,  # The rough ammount of data in a fileset to be picked up by a rsync thread
        'rsyncFlags': '-a',  # Flags to pass to rsync
        'folders': '/backMeUp1,/backMeUp2',  # Comma seperated list of filesystem locations to backup
        'drives': 'sda,sdb',  # Comma seperated list of disk devices where the data to be backed up exists
    }
    rsync = rsyncer(backupInfo)
    rsync.queLoad()
    while True:
        rsync.threadWatcher()
        queueSize = rsync.que.qsize()
        threads = rsync.tpool.numActive()
        if threads == 0 and queueSize == 0:
            break
    rsync.waitForQueue()
    print("Sync complete")


if __name__ == '__main__':
    main()
