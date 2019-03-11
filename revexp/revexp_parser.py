from joblib import Parallel, delayed
from lxml import etree, objectify
import os, csv, threading, time
from threading import Thread
from queue import Queue

csv.register_dialect('csvCommaDialect', delimiter='|', lineterminator='\n')


class Task(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try:
                func(*args, **kwargs)
            except Exception as e:
                print(e)
            finally:
                self.tasks.task_done()


class ThreadPool:
    """Pool of threads consuming tasks from a queue"""

    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Task(self.tasks)

    def add_task(self, func, *args, **kwargs):
        """Add a task to the queue"""
        self.tasks.put((func, args, kwargs))

    def wait_completion(self):
        """block until all tasks are done"""
        self.tasks.join()


def get_files(path):
    file_array = []
    for top, dirs, files in os.walk(path):
        for nm in files:
            file_array.append(os.path.join(top, nm))
    return file_array


def get_zip_files(path):
    zip_files = []
    for top, dirs, files in os.walk(path):
        for nm in dirs:
            zip_files += get_files(os.path.join(path, nm, 'orderClause'))
    return zip_files


def try_catch_decorator(my_func):
    def wrapper(value):
        try:
            val = value.text
            return my_func(val)
        except:
            val = ''
            return my_func(val)

    return wrapper


def try_except_decorator(my_func):
    def wrapper(value1, value2):
        try:
            val1 = value1.tag
            return my_func(value1, value2)
        except:
            try:
                val2 = value2.tag
                return my_func(value1, value2)
            except:
                value1 = ''
                value2 = ''
                return my_func(value1, value2)

    return wrapper


@try_except_decorator
def try_except(val1, val2):
    content = ''
    if val1 is not None:
        content = val1
    else:
        content = val2
    return content


@try_catch_decorator
def try_catch(value):
    content = value.replace('\n', '').replace('\r', '').replace('\t', '').replace('|', '/').replace('"', '')
    return content


def clean_name_spaces(root):
    for elem in root.getiterator():
        if not hasattr(elem.tag, 'find'):
            continue  # (1)
        i = elem.tag.find('}')
        if i >= 0:
            elem.tag = elem.tag[i + 1:]

    objectify.deannotate(root, cleanup_namespaces=True)

    return root


def get_value(tag):
    value = ''
    try:
        value = tag.replace('\n', '').replace('\r', '').replace('\t', '').replace('|', '/') \
            .replace('"', '').replace('  ', '').replace('NULL', '').replace(';', ',')
    except:
        value = ''
    return value


def parse(**kwargs):
    entity_list = []
    try:
        doc = etree.parse(kwargs['file'])
        root = clean_name_spaces(doc.getroot())
        for document in root.iter('Документ'):
            entity = {}
            entity['ИдДок'] = get_value(document.get('ИдДок'))
            entity['ДатаДок'] = get_value(document.get('ДатаДок'))
            entity['ДатаСост'] = get_value(document.get('ДатаСост'))

            try:
                entity['ИННЮЛ'] = get_value(try_except(document.find('СведНП'), '').get('ИННЮЛ'))
            except:
                entity['ИННЮЛ'] = ''

            try:
                entity['НаимОрг'] = get_value(try_except(document.find('СведНП'), '').get('НаимОрг'))
            except:
                entity['НаимОрг'] = ''

            try:
                entity['СумДоход'] = get_value(try_except(document.find('СведДохРасх'), '').get('СумДоход'))
            except:
                entity['СумДоход'] = ''

            try:
                entity['СумРасход'] = get_value(try_except(document.find('СведДохРасх'), '').get('СумРасход'))
            except:
                entity['СумРасход'] = ''

            entity_list.append(entity)
            print(entity['ИдДок'], entity['ИННЮЛ'], entity['ДатаДок'])

    except:
        pass

    if len(entity_list) > 0:
        kwargs['mutex'].acquire()
        with open('fns_data.csv', 'at', encoding='cp1251', errors='ignore') as file:
            writer = csv.DictWriter(file, ['ИдДок', 'ДатаДок', 'ДатаСост', 'ИННЮЛ', 'НаимОрг', 'СумДоход', 'СумРасход'],
                                    dialect='csvCommaDialect')
            writer.writerows(entity_list)
        kwargs['mutex'].release()


def create_thread_parser(thread_count, xml_files):
    mutex = threading.Lock()
    pool = ThreadPool(int(thread_count))
    while len(xml_files) != 0:
        xml_file = xml_files.pop()
        pool.add_task(parse, file=xml_file, mutex=mutex)
    pool.wait_completion()

    return None


a = os.getcwd()
t0 = time.time()
xml_files_list = get_files('C:/tmp')
create_thread_parser(30, xml_files_list)
t = time.time() - t0
print('Понадобилось времени (сек.): %fs' % t)
with open('time_log.log', 'at', encoding='cp1251', errors='ignore') as file:
    file.write('Понадобилось времени (сек.): %fs' % t + '\n')
    file.flush()
