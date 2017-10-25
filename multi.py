#!/usr/bin/python
import os
import shutil
import datetime as dt
import time
import Queue
from threading import Thread


def do_work(q, curr_thread):

    dest1_path = '/root/Documents/destcsv'
    dest2_path = '/root/Documents/destxls'
    dest3_path = '/root/Documents/desttxt'
    dest4_path = '/root/Documents/destof'


    while True:
        #get message from Queue
        filename = q.get()
        #compose the full path
        date = dt.datetime.now().strftime('%Y%m%d%H%M%S%f')
        source_full_path = ('{}/{}'.format(source_path,filename))
        dest_filename = ('{}-{}'.format(date,filename))
        #csv
        if filename.endswith("csv"):
            dest_full_path = ('{}/{}'.format(dest1_path,dest_filename))
            shutil.move(source_full_path, dest_full_path)
            print '{} ha processato un file csv'.format(curr_thread)
        #xls
        elif filename.endswith("xls"):
            dest_full_path = ('{}/{}'.format(dest2_path,dest_filename))
            shutil.move(source_full_path, dest_full_path)
            print '{} ha processato un file xls'.format(curr_thread)
        #txt
        elif filename.endswith("txt"):
            dest_full_path = ('{}/{}'.format(dest3_path,dest_filename))
            shutil.move(source_full_path, dest_full_path)
            print '{} ha processato un file txt'.format(curr_thread)
        #other
        else:
            dest_full_path = ('{}/{}'.format(dest4_path,dest_filename))
            shutil.move(source_full_path, dest_full_path)
            print '{} ha processato un file sconosciuto'.format(curr_thread)

def main():

    # queue initialization
    q = Queue.Queue()

    #threads initialization
    curr_thread = 0
    max_thread = 2
    while  curr_thread < max_thread :

        #thread initialization
        tw = Thread(target=do_work, args=(q, curr_thread))
        tw.setDaemon(True)
        tw.start()

        #increment curr_thread
        curr_thread += 1

    #define source directory
    global source_path
    source_path = '/root/Documents/source'
    while True:
        for dirname,filename,filenames in os.walk(source_path):
            for filename in filenames:
                q.put(filename)

        time.sleep(5)


if __name__ == '__main__':
    main()