# Do all imports for script. If this fails, you need to import more
from os import listdir, remove
import json
from os.path import join, isfile, basename, exists, normpath, sep
from csv import DictReader
import logging
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, DateTime, Date, MetaData, ForeignKey
from sqlalchemy.sql import text
from time import time
import regex as re
import traceback
from urllib.parse import unquote

files = []
root_folder = 'input'
for filename in listdir(root_folder):
    f = join(root_folder, filename)
    if isfile(f):
        files.append(f)

droid_headers_id = ["id", "parent_id", "uri", "file_path", "filename", "id_method", "status", "size", "type", "file_extension", "last_modified", "ext_mis_warning", "hash", "file_format_count"]

droid_int_headers = ["id", "parent_id", "size"]
droid_date_headers = ["last_modified"]

droid_headers_format = ["id", "file_id", "pronom_id", "mime_type", "file_format_name", "file_format_version"]

key_to_header = {
    'id':                 'id',
    'parent_id':          'parent_id',
    'uri':                'uri',
    'file_path':          'file_path',
    'name':               'filename',
    'method':             'id_method',
    'status':             'status',
    'size':               'size',
    'type':               'type',
    'ext':                'file_extension',
    'last_modified':      'last_modified',
    'extension_mismatch': 'ext_mis_warning',
    'sha256_hash':        'hash',
    'format_count':       'file_format_count',
    'puid':               'pronom_id',
    'mime_type':          'mime_type',
    'format_name':        'file_format_name',
    'format_version':     'file_format_version',
}

path_regex = re.compile('[0-9]{4}[_-]semester[_-][123]|to-be-sorted-by-semester')

def insert_data(table, values):
    inserted = False
    current_valid = 0
    step = len(values)
    stop = len(values)
    while not inserted and step > 0:
        try:
            conn.execute(table, values[current_valid:min(stop, current_valid + step)])
            current_valid += step
            if current_valid >= stop:
                inserted = True
        except Exception as e:
            if step == 1:
                print(f"Failed on record #{current_valid}", values[current_valid:current_valid + step])
                print(e)
                step = 0
                break
            else:
                print(f"Failed on step {step}. Quartering step and trying again.")
                step = max(int(step / 4), 1)

def find_database():
    names = []
    for file in listdir():
        if file.startswith('ETC_Droid_DB_'):
            names.append(file)
    if len(names) == 0:
        time = datetime.now()
        return f'ETC_Droid_DB_{time.strftime("%Y%m%d%H%M%S")}.db'
    else:
        raise Exception('Error. You have not moved/deleted the old database!!!')

# Create the sql engine
handler = logging.FileHandler('sql.log')
handler.setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy').addHandler(handler)

engine = create_engine(f"sqlite:///{find_database()}", echo=False)
metadata = MetaData()

droid_ids = Table('droid_ids', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('parent_id', Integer),
                  Column('uri', String),
                  Column('file_path', String),
                  Column('filename', String),
                  Column('id_method', String),
                  Column('status', String),
                  Column('size', Integer),
                  Column('type', String),
                  Column('file_extension', String),
                  Column('last_modified', DateTime),
                  Column('ext_mis_warning', String),
                  Column('hash', String),
                  Column('file_format_count', Integer),
                  Column('project_name', String),
                  Column('project_year', Integer),
                  Column('project_semester', Integer))

droid_formats = Table('droid_formats', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('file_id', Integer, ForeignKey('droid_ids.id')),
                      Column('pronom_id', String),
                      Column('mime_type', String),
                      Column('file_format_name', String),
                      Column('file_format_version', String))

metadata.create_all(engine)
conn = engine.connect()

# Map the ID values into a new dict w/ parsed values
def map_droid_dict_id_values(row):
    row_dict = {}
    for k, value in row.items():
        if not k:
            continue
        key = key_to_header[k.lower()]

        if key in droid_int_headers:
            row_dict[key] = int(value) if value else 0
        elif key in droid_date_headers:
            row_dict[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S') if value else datetime.today()
        elif key in droid_headers_id:
            row_dict[key] = value
    return row_dict


# Map the Format values into a new dict w/ parsed values
def map_droid_dict_format_values(row):
    new_row_dict_format = {}
    for k, value in row.items():
        if not k:
            continue
        key = key_to_header[k.lower()]

        if key == 'id':
            new_row_dict_format[key] = value
        elif key == 'file_id':
            new_row_dict_format[key] = int(value) if value else 0
        elif key in droid_headers_format:
            new_row_dict_format[key] = value
    return new_row_dict_format


def parsepath(path):
    path = path.replace('\\', '/')
    if path.endswith('/'):
        path = path[:-1]
    return path


def parseprojectpath(path):
    path = parsepath(path)
    path = path.split('/')
    while len(path) > 0 and not path_regex.fullmatch(path[0]):
        path = path[1:]
    return '/' + '/'.join(path)

def parsefoldername(path):
    path = parsepath(path)
    return basename(path)


# Create the project name to project file name dict_link
def parse_project_listing_csv(input_file):
    parsed_names = {}
    with open(input_file, 'r', encoding='utf-8') as file:
        dict_reader = DictReader(file)
        for row in dict_reader:
            project_name = row['Project Name']
            project_path = row['Parent File Path']
            project_year = row['Year']
            project_semester = row['Semester']
            if project_path == "":
                continue
            folder_name = parsefoldername(project_path)
            parsed_names[folder_name] = project_name, project_year, project_semester
    return parsed_names


project_name_by_folder_name = parse_project_listing_csv('ETC_Past_Projects_Listing.csv')


# Parse project name from file path
def parse_project_name(file_path):
    folder_name = basename(file_path)
    if folder_name in project_name_by_folder_name:
        return project_name_by_folder_name[folder_name]
    else:
        return f'Unverified Name: {folder_name}', '', ''


def get_time(start_time, end_time):
    runtime = int(end_time - start_time)
    hours = int(runtime / 3600)
    minutes = int((runtime % 3600) / 60)
    seconds = (runtime % 3600) % 60
    return f'{hours:2d}h {minutes:2d}m {seconds:2d}s'


def print_progress(stime, progress, pmax, end_message=None, erase=False, step=25):
    if erase:
        print('\r[', end='', flush=True)
    else:
        print('[', end='', flush=True)
    for x in range(int(progress / pmax * step)):
        print('=', end='', flush=True)
    for x in range(step - int(progress / pmax * step)):
        print(' ', end='', flush=True)
    if end_message is None:
        print('] {0:3d}% {1}'.format(int(progress / pmax * 100), get_time(stime, time())), end='', flush=True)
    else:
        print(f'] {end_message}', flush=True)


# Create arrays of the ID and Format rows
def insert_dict_list(csv_files):
    progress = 0
    count = 0
    format_id = 0

    file_count = len(csv_files)
    start_time = time()

    print_progress(start_time, progress, file_count)

    file_step = max(int(file_count / 4), 1)
    for x in range(0, file_count, file_step):
        for file in csv_files[x:x + file_step]:
            output_ids, output_formats = [], []

            # Try to get data
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    dict_reader = DictReader(f)

                    project_name = ''
                    max_id = 0
                    for row in dict_reader:
                        if row['URI'].endswith('./'):
                            continue
                        if row['FILE_PATH'] == '':
                            uri = row['URI']
                            row['FILE_PATH'] = unquote(uri[uri.index('file://') + len('file://'):])
                        try:
                            row_id = map_droid_dict_id_values(row)
                            row_format = map_droid_dict_format_values(row)

                            if row_id['id'] == 2:
                                project_name, project_year, project_semester = parse_project_name(row_id['file_path'])

                            max_id = max(row_id['id'], max_id)

                            row_id['id'] += count
                            row_id['project_name'] = project_name
                            row_id['project_year'] = project_year
                            row_id['project_semester'] = project_semester

                            row_id['file_path'] = parseprojectpath(row_id['file_path'])
                            row_id['uri'] = 'file:' + row_id['file_path']

                            row_format['id'] = format_id + count
                            row_format['file_id'] = row_id['id']
                            format_id += 1

                            output_formats.append(row_format)

                            format_count = 1
                            if None in row:
                                extra = row[None]
                                for x in range(0, len(extra), 4):
                                    temp_row_format = {
                                        'id':                  format_id + count,
                                        'file_id':             row_id['id'],
                                        'pronom_id':           extra[x],
                                        'mime_type':           extra[x + 1],
                                        'file_format_name':    extra[x + 2],
                                        'file_format_version': extra[x + 3]
                                    }
                                    output_formats.append(temp_row_format)
                                    format_id += 1
                                    format_count += 1
                            row_id['format_count'] = format_count
                            output_ids.append(row_id)
                        except Exception as e:
                            print_progress(None, progress, file_count, erase=True, end_message='Failed!')
                            raise Exception(f'\nFailed on row {row}\n{traceback.format_exc()}')
                    count += max_id
            except Exception as e:
                print_progress(None, progress, file_count, erase=True, end_message='Failed!')
                print(f'\nFailed on project {file}!\n{traceback.format_exc()}')

            # Try to insert the current data progress
            ins_ids = droid_ids.insert()
            ins_formats = droid_formats.insert()

            # Droid_Ids
            insert_data(ins_ids, output_ids)
            # Droid_Formats
            insert_data(ins_formats, output_formats)

            del output_ids
            del output_formats
            progress += 1
            print_progress(start_time, progress, file_count, erase=True)
    print_progress(None, progress, file_count, erase=True, end_message='Complete!')
    print(f'Processing took {get_time(start_time, time())}.')

# Insert the data into the tables
insert_dict_list(files)

del project_name_by_folder_name