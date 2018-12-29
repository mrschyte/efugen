import click
import csv
import logging
import os.path
import os
import queue
import stat
import threading
import time

def parwalk(paths, nthreads=4):
    def worker(tasks, results):
        while True:
            path = tasks.get()
            try:
                for entry in os.scandir(path):
                    abspath = os.path.join(path, entry.name)
                    if entry.is_dir() and not entry.is_symlink():
                        tasks.put(abspath)
                    results.put((abspath, os.stat(abspath)))
            except Exception as ex:
                pass
            tasks.task_done()

    results = queue.Queue()
    tasks = queue.Queue()
    threads = []

    for path in paths:
        tasks.put(path)

    for _ in range(nthreads):
        thread = threading.Thread(target=worker, args=(tasks, results))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    while True:
        while not results.empty():
            yield results.get()

        # avoid excessive locking
        time.sleep(1)

        with tasks.all_tasks_done:
            if not tasks.unfinished_tasks:
                break

def efugen(threads, paths, relpath, prepend, output):
    def convtime(ns):
        return 0x019DB1DED53E8000 + ns // 100

    with open(output, 'w', encoding='utf-8', errors='surrogateescape') as fp:
        dw = csv.DictWriter(fp, quoting=csv.QUOTE_NONNUMERIC,
                            fieldnames=['Filename',
                                        'Size',
                                        'Date Modified',
                                        'Date Created',
                                        'Attributes'])

        dw.writeheader()

        for processed, (full_path, attrib) in enumerate(parwalk(paths, threads)):
            if relpath:
                full_path = os.path.relpath(full_path, relpath)

            if prepend:
                full_path = prepend + full_path

            full_path = full_path.replace('/', '\\')

            if stat.S_ISLNK(attrib.st_mode):
                mode = 0x400 # FILE_ATTRIBUTE_REPARSE_POINT
            elif stat.S_ISDIR(attrib.st_mode):
                mode = 0x010 # FILE_ATTRIBUTE_DIRECTORY
            else:
                mode = 0x000 # REGULAR_FILE

            dw.writerow({'Filename': full_path,
                            'Size': attrib.st_size,
                            'Date Modified': convtime(attrib.st_mtime_ns),
                            'Date Created': convtime(attrib.st_ctime_ns),
                            'Attributes': mode})

            if processed % 1000 == 0:
                logging.info('Processed: [{0:12d}], "{1}"'.format(processed, full_path))

@click.command()
@click.option('--threads', default=4)
@click.option('--relpath', default=None)
@click.option('--prepend', default=None)
@click.option('--output', required=True)
@click.argument('paths', nargs=-1)
def main(threads, relpath, prepend, output, paths):
    logging.basicConfig(level=logging.INFO)
    efugen(threads, paths, relpath, prepend, output)

if __name__ == '__main__':
    main()
