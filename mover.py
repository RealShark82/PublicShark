#!/usr/bin/python
def mover():
    import os,shutil
    import datetime as dt
    source_path = '/root/Documents/source'
    dest1_path = '/root/Documents/destcsv'
    dest2_path = '/root/Documents/destxls'
    dest3_path = '/root/Documents/desttxt'
    dest4_path = '/root/Documents/destof'
    while True:
        for dirname, dirnames, filenames in os.walk(source_path):
            for filename in filenames:
                date = dt.datetime.now().strftime('%Y%m%d%H%M%f')
                print filename,dirname
                source_full_path = os.path.join(dirname,filename)
                print source_full_path
                dest_filename = ('{}-{}'.format(date, filename))
                print dest_filename
                if filename.endswith("csv"):
                    dest_full_path = os.path.join(dest1_path, dest_filename)
                    shutil.move(source_full_path, dest_full_path)
                elif filename.endswith("xls"):
                    dest_full_path = os.path.join(dest2_path, dest_filename)
                    shutil.move(source_full_path, dest_full_path)
                elif filename.endswith("txt"):
                    dest_full_path = os.path.join(dest3_path, dest_filename)
                    shutil.move(source_full_path, dest_full_path)
                else:
                    dest_full_path = os.path.join(dest4_path, dest_filename)
                    shutil.move(source_full_path, dest_full_path)


if __name__ == '__main__':
    mover()

