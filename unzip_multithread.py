#!/usr/bin/env python
#<span style="font-size:14px;"># -*- coding: utf-8 -*-
import os
import sys
import shutil
import threading
import linecache
from time import ctime, sleep
from multiprocessing import cpu_count
import pdb
pdb.set_trace()

'''

http://blog.csdn.net/daiyinger/article/details/48289575
'''


log_mutex = threading.Lock()
list_mutex = threading.Lock()
stdout_mutex = threading.Lock()

def single_command(in_entry, key_len):
    #used for kaldi scp file generation
    if len(in_entry) == 0:
        return

    part_dir_len = key_len # this can be set by author
    entry_dir = os.path.dirname(in_entry)
    dir_units = [x for i in entry_dir.split(os.path.sep) if len(i) > 0]
    unit_num = len(dir_units)
    ii = 0
    key_id = r""
    while ii < part_dir_len:
        key_id += dir_units[unit_num - ii - 1] + r"_"
        ii += 1

    out_entry = key_id + "\t" + in_entry
    return out_entry


def command_execuator(entries, log_fp, thread_index, start, end):
    if end <= start or log_fp is None or len(entries) == 0 or thread_index < 0:
        sys.stdout.write("Condition maybe somewhat error !\n")
        return
    entry_start = start
    succ_num = 0
    while entry_start <= end:
        curr_task = entries[entry_start]
        curr_task = curr_task.strip("\n\r")
        task_destination = os.path.dirname(curr_task)
        if not os.path.exists(task_destination):
            os.makedirs(task_destination, mode=0o777)

        cmd = "unzip -f -o -d  %s -d %s" % (task_destination, curr_task)
        if stdout_mutex.acquire():
            print ("Thread : " + str(thread_index) + ":" + cmd + '\n')
            stdout_mutex.release()
        if os.system(cmd) != 0:
            if log_mutex.acquire():
                log_fp.write("Thread : " + str(thread_index) + ":" + curr_task + "\n")
                log_mutex.release()
            exit(1)
        succ_num += 1
        entry_start += 1
        
    print "Thread-%s:%s" % (thread_index, succ_num)
    if log_mutex.acquire():
        log_fp.write("Thread : " + str(thread_index) + " : in : " + str(end-start+1) + " out : " + str(succ_num) + "\n")
    log_mutex.release()

if __name__ == "__main__":
    #
    command_list = r""
    if len(sys.argv) > 1:
        command_list = str(sys.argv[1])
    else:
        sys.stdout.write(r"No enough parameter here, system will exit !\n")
        exit(-1)

    entries = linecache.getlines(filename=command_list)
    total_entry_num = len(entries)

    log_fp = open("command_run_log.txt", 'w')
    total_core_num = cpu_count()
    task_num_one_thread = int(total_entry_num/total_core_num) + 1
    t = ['None'] * total_core_num
    for thread_index in range(0, total_core_num-1):
        entry_start = thread_index * task_num_one_thread
        entry_end = (thread_index + 1) * task_num_one_thread - 1
        if entry_end >= total_entry_num:
            entry_end = total_entry_num

        t[thread_index] = threading.Thread(target=command_execuator, args=(entries, log_fp, thread_index, entry_start, entry_end))
        # 启动
        t[thread_index].start()
        t[thread_index].join()

    log_fp.close()
