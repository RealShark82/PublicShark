#!/usr/bin/python
import os
import shutil
import time
import Queue
from threading import Thread
import hashlib


def do_work(q, curr_thread, source_path):

    dest1_path = '/root/Documents/destcsv'
    dest2_path = '/root/Documents/destxls'
    dest3_path = '/root/Documents/desttxt'
    dest4_path = '/root/Documents/destof'
    dest5_path = '/root/Documents/destcopy'


    while True:

        #get message from Queue
        filename = q.get()

        #compose the full path
        source_full_path = os.path.join(source_path,filename)

        #csv
        if filename.endswith("csv"):
            dest_full_path = os.path.join(dest1_path,filename)

            #check if a file already exist
            #if not exist
            if not os.path.isfile(dest_full_path):
                shutil.move(source_full_path,dest_full_path)

            #if exist
            elif not hash(source_full_path) == hash(dest_full_path):

                #add suffix i to file
                i = 0
                dest_full_path = os.path.join(dest1_path, "%d%s" %(i, filename))

                #if exist suffix i++
                while os.path.isfile(dest_full_path):
                    i += 1
                    dest_full_path = os.path.join(dest1_path,"%d%s" % (i,filename))
                shutil.move(source_full_path,dest_full_path)

            #move to duplicate file
            else:
                dest_full_path = os.path.join(dest5_path,filename)
                shutil.move(source_full_path,dest_full_path)
            print '{} ha processato un file csv'.format(curr_thread)

        #xls
        elif filename.endswith("xls"):
            dest_full_path = os.path.join(dest2_path,filename)

            # check if a file already exist
            # if not exist
            if not os.path.isfile(dest_full_path):
                shutil.move(source_full_path,dest_full_path)

            # if exist
            elif not hash(source_full_path) == hash(dest_full_path):

                # add suffix i to file
                i = 0
                dest_full_path = os.path.join(dest2_path,"%d%s" % (i,filename))

                # if exist suffix i++
                while os.path.isfile(dest_full_path):
                    i += 1
                    dest_full_path = os.path.join(dest2_path,"%d%s" % (i,filename))
                shutil.move(source_full_path,dest_full_path)

            # move to duplicate file
            else:
                dest_full_path = os.path.join(dest5_path,filename)
                shutil.move(source_full_path,dest_full_path)
            print '{} ha processato un file csv'.format(curr_thread)

        #txt
        elif filename.endswith("txt"):
            dest_full_path = os.path.join(dest3_path,filename)

            # check if a file already exist
            # if not exist
            if not os.path.isfile(dest_full_path):
                shutil.move(source_full_path,dest_full_path)

            # if exist
            elif not hash(source_full_path) == hash(dest_full_path):

                # add suffix i to file
                i = 0
                dest_full_path = os.path.join(dest3_path,"%d%s" % (i,filename))

                # if exist suffix i++
                while os.path.isfile(dest_full_path):
                    i += 1
                    dest_full_path = os.path.join(dest3_path,"%d%s" % (i,filename))
                shutil.move(source_full_path,dest_full_path)

            # move to duplicate file
            else:
                dest_full_path = os.path.join(dest5_path,filename)
                shutil.move(source_full_path,dest_full_path)
            print '{} ha processato un file csv'.format(curr_thread)

        #other
        else:
            dest_full_path = os.path.join(dest4_path,filename)

            # check if a file already exist
            # if not exist
            if not os.path.isfile(dest_full_path):
                shutil.move(source_full_path,dest_full_path)

            # if exist
            elif not hash(source_full_path) == hash(dest_full_path):

                # add suffix i to file
                i = 0
                dest_full_path = os.path.join(dest4_path,"%d%s" % (i,filename))

                # if exist suffix i++
                while os.path.isfile(dest_full_path):
                    i += 1
                    dest_full_path = os.path.join(dest4_path,"%d%s" % (i,filename))
                shutil.move(source_full_path,dest_full_path)

            # move to duplicate file
            else:
                dest_full_path = os.path.join(dest5_path,filename)
                shutil.move(source_full_path,dest_full_path)
            print '{} ha processato un file csv'.format(curr_thread)


def hash(fpth):

    #create the hash of the file in fpth
    with open(fpth, 'rb') as f:
        m = hashlib.md5()

        #read datablock of 8192
        while True:
            data = f.read(8192)
            if not data:
                break
            m.update(data)

        #return the hexhash
        return m.hexdigest()
    print m.hexdigest



def main():

    #define source directory
    source_path = '/root/Documents/source'
    # queue initialization
    q = Queue.Queue()

    #threads initialization
    curr_thread = 0
    max_thread = 2
    while  curr_thread < max_thread :

        #thread initialization
        tw = Thread(target=do_work, args=(q, curr_thread, source_path))
        tw.setDaemon(True)
        tw.start()

        #increment curr_thread
        curr_thread += 1

    #put filename in Queue
    while True:
        for dirname,filename,filenames in os.walk(source_path):
            for filename in filenames:
                q.put(filename)

        time.sleep(5)


if __name__ == '__main__':
    main()