'''
Droid Wrapper for python
	 Authored by Ethan Wolfe
	 
	 Usage:
		droid.py <S|M> <T|S|P> <Working Dir> <Output Dir> <Blacklisted Names>
		droid.py <R> <Output Dir>
	 
     CSV Type
        S (Single) Generate a single csv output. Doesn't affect Project-Level scans
        M (Multiple) Generate a csv file for every project level folder found
     
	 Level:
		The level of the working_dir that you are giving it.
		T (Top-Level) - e.g. `D:/`
		S (Semester-Level) - e.g. `D:/2017_semester_1`
		P (Project-Level) - e.g. `D:/2017_semester_1/wfk`
		R (Restart Crashed)
	
	Working Dir:
		The directory that you want the program to search and call droid on
	
	Output Dir:
		The output directory that you want the program to generate output folders into
	
	Blacklisted Names:
		This is list of names that you don't want to be scanned. 
		For a project level scan, other than the project name, there is nothing to blacklist.
		For a Semester level scan, blacklsit names would be project folder names
		For a Top level scan, a blacklist name could either be a semester folder name, a project folder name or a combination of the two. e.g. 2018_semester_2 or 2018_semester_2/arthmagic
		
	 Requires droid to be installed into the default directory.
	 
	 It is advised that you clear out your ~/.droid file of profiles to avoid
	 conflicts caused by old droid profiles that might have the same profile id after generation. This will also save you some much needed disk space
'''

from os import listdir, getcwd, mkdir, remove
from os.path import expanduser, join, exists, normpath, basename, dirname
from subprocess import Popen, PIPE
from shutil import copyfile, move
from sys import argv
from time import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging as log
import platform

if platform.system() == 'Windows':
    DROID_LOCATION = expanduser('~\Droid\droid.bat')
    SIGNATURE_FILE = getcwd() + '\DROID_SignatureFile_V97.xml'
    DROID_PROFILE = getcwd() + '\HashProfile.droid'
else:
    DROID_LOCATION = expanduser('~/Droid/droid.sh')
    SIGNATURE_FILE = getcwd() + '/DROID_SignatureFile_V97.xml'
    DROID_PROFILE = getcwd() + '/HashProfile.droid'

def get_scan_dirs(working_dir, output, level, blacklisted):
    # Figure out which direcotries we want to be looking at
    scan_dirs = []
    if level == 'P':
        scan_dirs.append(basename(normpath(working_dir)))
    elif level == 'S':
        scan_dirs += listdir(working_dir)
    else:
        for directory in listdir(working_dir):
            if directory not in blacklisted:
                for sub_dir in listdir(join(working_dir, directory)):
                    if sub_dir not in blacklisted:
                        scan_dirs.append(join(directory, sub_dir).replace('/', '.'))
    # Remove blacklisted folders
    for directory in blacklisted:
        if directory in scan_dirs:
            scan_dirs.remove(directory)
    
    # Remove folders that finished and Delete droid files that did not generate properly
    pre_finished = 0
    for file in listdir(output):
        if file.endswith('.droid'):
            if not 'working' in file:
                scan_dirs.remove(file[:-6].replace('.', '/'))
                pre_finished += 1
            else:
                remove(join(output, file))
    return pre_finished, scan_dirs


def create_output_folder(output_dir):
    output = join(output_dir, f'droid_output_{int(time())}')
    mkdir(output)
    return output

def create_metadata_file(working_dir, output, blacklisted, level, gen_type):
    # Generate a metadata file in the output for resume on crash
    with open(join(output, 'metadata.txt'), 'w') as file:
        file.write(working_dir + '\n')
        file.write(output + '\n')
        if len(blacklisted) > 0:
            file.write(blacklisted[0])
            for directory in blacklisted[1:]:
                file.write(',' + directory)
        file.write('\n')
        file.write(level + '\n')
        file.write(gen_type)


def main(working_dir, output, blacklisted, csv_type, level, crashed):
    log.info(f'Storing output in {output}')
    # Take stock of when we start
    start_time = time()
    log.info(f'Started processing at {start_time}')
    
    if not crashed:
        create_metadata_file(working_dir, output, blacklisted, level, csv_type)
    
    
    pre_finished, scan_dirs = get_scan_dirs(working_dir, output, level, blacklisted)
    
    if level == 'P': # Account for Project level working change
        working_dir = dirname(working_dir)

    # Start the droid scanning of the dirs
    if len(scan_dirs) > 0:
        finished = 0
        with ThreadPoolExecutor() as executor:
            futures = []
            for directory in scan_dirs:
                futures.append(executor.submit(call_droid, output=output, working=working_dir, csv_type=csv_type, path=directory))
            for future in as_completed(futures):
                if not future.result().startswith('Error'):
                    finished += 1
                log.info(f'{future.result()} - {finished + pre_finished} / {len(scan_dirs) + pre_finished}')
    else:
        log.info('Already finished all droid scanning. Going to CSV generation.')
    
    if finished != len(scan_dirs):
        log.info('Encountered an error while running. Please fix the problem and rerun the program to try again.')
        exit_program(start_time, 1)
    else:
        if csv_type == 'S':
            # Call droid to generate the csv file from the resultant droid files
            log.info('Generating CSV file')
            process = Popen([DROID_LOCATION, '-p'] + [join(output, path + '.droid') for path in scan_dirs] + ['-e', join(output, 'output.csv')], stdout=PIPE, stderr=PIPE)
            stdout, stderr = process.communicate()
            
            if stderr != b'':
                log.error(f'{stderr}\nError while generating CSV file. Please fix problem and try again.')
                remove(join(output, 'output.csv'))
                exit_program(start_time, 1)
            else:
                log.info('Finished Generating CSV file successfully.')
                remove(join(output, 'metadata.txt'))
                exit_program(start_time, 0)
        else:
            exit_program(start_time, 0)

def exit_program(start_time, error_code):
    end_time = time()
    log.info('Finished processing at {end_time}')
    runtime = int(end_time-start_time)
    hours = runtime / 3600
    minutes = (runtime % 3600) / 60
    seconds = (runtime % 3600) % 60
    log.info(f'Processing took {hours}h {minutes}m {seconds}s.')
    if error_code == 0:
        log.info('Exiting')
    else:
        log.error('Exiting with Error')
    quit(error_code)

def call_droid(output, working, csv_type, path):
    log.info(f'Started Scanning {path}')
    profile = join(output, path + '_working.droid')
    log.info(f'Copying droid profile {profile}')
    copyfile(DROID_PROFILE, profile)
    # Call droid to populate that profile with the results
    process = Popen([DROID_LOCATION, '-R', '-q', '-W', '-A', '-a', join(working, path.replace('.', '/')), '-p', profile], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    # If we want individual csvs, then generate them here
    if csv_type == 'M':
        log.info(f'Generating CSV for {path}')
        process = Popen([DROID_LOCATION, '-p', profile, '-e', join(output, path + '.csv')], stdout=PIPE, stderr=PIPE)
        stdout, stderr = process.communicate()
    if stderr != b'':
        log.error(stderr.decode('utf-8'))
        return "Error while Scanning " + path
    else:
        # After we have successfully run the code, move from working to normal
        finished_profile = join(output, path + '.droid.')
        move(profile, finished_profile)
        return "Finished Scanning " + path

if __name__ == "__main__":
    if len(argv) == 3 and argv[1] == 'R':
        output_folder = argv[2]
        found_folders = []
        for folder in listdir(output_folder):
            if folder.startswith('droid_output'):
                found_folders.append(folder)
        if len(found_folders) > 0:
            filename = join(output_folder, found_folders[-1], 'metadata.txt')
            if exists(filename):
                with open(filename, 'r') as file:
                    working_dir = file.readline().strip()
                    output = file.readline().strip()
                    blacklisted = file.readline().split(',')
                    level = file.readline()
                    gen_type = file.readline()
                    log.basicConfig(filename=f'{output}/log.txt', encoding='utf-8', level=log.INFO)
                    main(working_dir, output, blacklisted, gen_type, level, True)
            else:
                print('Last program did not crash.')
                quit(1)
        else:
            print('No crashed programs found.')
            quit(1)
        
    else:
        if len(argv) < 4:
            print("Invalid Number of Arguments.")
            quit(1)
        if argv[1] not in ['S', 'M']:
            print('Invalid CSV Type Argument')
            quit(1)
        if argv[2] not in ['T', 'S', 'P']:
            print('Invalid Level Argument')
            quit(1)
        output = create_output_folder(argv[4])
        log.basicConfig(filename=f'{output}/log.txt', encoding='utf-8', level=log.INFO)
        main(argv[3], output, argv[5:], argv[1], argv[2], False)