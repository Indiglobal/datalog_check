import os
import collections
import datetime
import click
import sys
import glob


def process_data(data_path, ):
    print(f'Data path: {data_path}')    
    files = glob.glob(data_path + '/**/*.csv', recursive=True)

    print("Data files to check:")
    for file in files:
        print(f'\t{file}')

    raw_data = []
    for file in files: 
        realfile = os.path.join(data_folder, file)
        with open(realfile, 'r', encoding='utf_16_le') as f:
            for line in range(10):
                try:
                    line_r = f.readline()
                except:
                    print(f'Problem with line: {line} in {file}')
            line = f.readline()
            while line:
                line_data = line.split(',')
                line_data[2] = line_data[2].rstrip()
                line_data.pop(0)
                if len(line_data) > 2:
                    line_data.pop()
                line_data.append(file)
                raw_data.append(line_data)
                line = f.readline()
    print(f'Concatenated {len(files)} files') 

    time_stamps = [item[0] for item in raw_data]
    duplicates = []
    seen = set()
    unique_data = []

    print('Checking for duplicates')
    for time in raw_data:
        if time[0] not in seen:
            unique_data.append(time)
            seen.add(time[0])
        else:
            duplicates.append(time)
    with open('duplicates', 'w') as duplicates_file:
        for line in duplicates:
            duplicates_file.write(f'{line}\n')

    pressure_data = []

    print('Reformatting data')
    for time in unique_data:
        if '-' in time[0]:
            timestamp = datetime.datetime.strptime(time[0], '%Y-%m-%d %H:%M:%S:%f')
        else:
            timestamp = datetime.datetime.strptime(time[0], '%m/%d/%Y %H:%M:%S:%f')
        pressure_data.append([timestamp,float(time[1])])

    print(f'Original pressure data size: {len(raw_data)}')
    print(f'Unique pressure data size: {len(unique_data)}')
    with open('Complete Test Data.txt', 'w') as pressure_data_file:
        for data_point in pressure_data:
            pressure_data_file.write(f'{data_point[0]} -- {data_point[1]}psi\n')
            
    class data_point(object):
        def __init__(self, time, pressure):
            self.time = time
            self.pressure = pressure

    with open('Test data summary.txt', 'w') as test_summary:
        test_summary.write('Test Data Summary\n')
        test_summary.write(f'Total data points collected: {len(raw_data)}\n')
        test_summary.write(f'Duplicate data points found: {len(duplicates)}\n')
    #    test_summary.write(f'Duplicates:\n')
    #    for line in duplicates:
    #        test_summary.write(f'{line}\n') 


        low_state = False 
        low_threshold = 1
        low_duration = 1
        high_state = False
        high_threshold = 160
        high_duration = 60

        step = 'low'
        cycle_count = 0
        cycle_data = ''

        print('Counting cycles')
        for data_line in pressure_data:

            c_data_point = data_point(data_line[0],data_line[1])

            if  step == 'low':

                if c_data_point.pressure > low_threshold:
                    low_state = False
                elif c_data_point.pressure <= low_threshold and low_state is not True:
                    low_state = True
                    low_start = c_data_point.time
                elif low_state:
                    time_at_state = c_data_point.time - low_start  
                    #print(f'Time at state: {time_at_state}, Low start: {low_start}, Current time: {c_data_point.time}')
                    if time_at_state.total_seconds() >= low_duration:
                       step = 'high'
                       low_state = False
                       time_at_state = 0

            elif step == 'high':

                if c_data_point.pressure <= high_threshold:
                    high_state = False
                elif c_data_point.pressure > high_threshold and high_state is not True:
                    high_state = True
                    high_start = c_data_point.time
                elif high_state:
                    time_at_state = c_data_point.time - high_start 
                    if time_at_state.total_seconds() <= high_duration:
                        step = 'low'
                        high_state = False
                        time_at_state = 0
                        cycle_count += 1
                        print(f'Cycle {cycle_count} completed at {c_data_point.time}')
                        cycle_data += (f'Cycle {cycle_count} completed at {c_data_point.time}\n')
        test_summary.write(f'Cycles complete: {cycle_count}\n')
        test_summary.write(f'Cycle summaries:\n{cycle_data}')

        print(f'Cycles: {cycle_count}')         
            
if __name__ ==  '__main__':

    cwd = os.path.dirname(os.path.realpath(__file__))
    # print(__file__)
    # print(sys.argv)

    if len(sys.argv) > 1:
        data_folder = os.path.join(cwd, sys.argv[1])
    else:
        data_folder = os.path.join(cwd, 'pressure_files')

    process_data(data_folder)

