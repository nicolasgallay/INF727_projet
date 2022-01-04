import subprocess as subprocess #lance des processus lourd
import sys
import time
from itertools import zip_longest 
import shutil
import glob
import os
import math
import numpy as np
import pandas as pd
import json
    


def ssh(command,machines_names,login):
    listproc = []
    timer=100

    for names in machines_names:

        proc = subprocess.Popen(["ssh",login+"@"+names,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)
        
    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            
def scp_splits(localPath,distantPath,machines_names,login,split_list):
    listproc = []

    for index, names in enumerate(machines_names):
        proc = subprocess.Popen(["scp",localPath+split_list[index],login+"@"+names+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout") 

def scp(localPath,distantPath,machines_names,login):
    listproc = []

    for  names in machines_names:
        proc = subprocess.Popen(["scp",localPath,login+"@"+names+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout") 
  

def scp_reduceFiles(localPath, distantPath,machines_names,login):
    listproc = []
    sorted_dict  = {}

    for  names in machines_names:
        proc = subprocess.Popen(["scp",login+"@"+names+":"+localPath,distantPath+'reduce_all/'],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")

    for filename in glob.glob(distantPath +'reduce_all/*.txt'):
        with open(filename,'r',encoding='ISO-8859-1') as f:
            for line in f:
                (key, val) = line.split(' ')
                sorted_dict[str(key)] = int(val)
    sorted_dict = dict(sorted(sorted_dict.items(), key=lambda item: item[1], reverse=True)) 

    with open(distantPath+'output/output.txt', 'w',encoding='ISO-8859-1') as output_file:
        output_file.write(json.dumps(sorted_dict))  
                
            
def split_names(machines_names):
    split = []
    for  index, names in enumerate(machines_names):
        split.append('S'+str(index)+'.txt')
    return split
              


def split_equal(input_path, output_path,machines_names):
    size = os.path.getsize(input_path)
    chunk_size = int(np.ceil(size /len(split_names(machines_names))))
    with open(input_path, 'rb') as mfile:
        content = bytearray(os.path.getsize(input_path))
        mfile.readinto(content)
        
        for c, i in enumerate(range(0, len(content), chunk_size)):
            mfile.seek(0)
            with open(output_path+'S'+str(c)+'.txt', 'wb') as f:
                f.write(content[i: i + chunk_size])       
    return 
    
def split_machines_file(input_path, output_path, nbr_lines):
    with open(input_path, 'r') as myfile:
        head = [next(myfile) for x in range(nbr_lines)]
        with open(output_path, 'w', encoding= 'ISO-8859-1') as file:
            file.write(''.join(head))
    return


def scp_output(localPath, distantPath,machines_names,login):
    listproc = []
    sorted_dict  = {}

    for  names in machines_names:
        print(f"scp",login+"@"+names+":"+localPath,distantPath+'output/')
        proc = subprocess.Popen(["scp",login+"@"+names+":"+localPath,distantPath+'output/'],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")

def clean(file_path):
    listproc = []
    timer=100
    file = open('/tmp/ngallay-20/machines.txt','r')
    machines_names = file.read().splitlines()  
    for name in machines_names:
        proc = subprocess.Popen(['ssh',  'ngallay-20@' + name, 'rm -rf ', file_path],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")   


def main(machines_number):
    
    login="ngallay-20"   
    dict_time ={}


    with open(r'/tmp/ngallay-20/machines.txt','r') as file2:
        machines_all = file2.read().splitlines()  


    split_machines_file(r'/tmp/ngallay-20/machines.txt',r'/tmp/ngallay-20/job/machine_names.txt',machines_number)
    scp(r'/tmp/ngallay-20/job/machine_names.txt',r'/tmp/ngallay-20/job/',machines_all,login)
    file = open('job/machine_names.txt','r')
    machines_names = file.read().splitlines()  
    split_list = split_names(machines_names) 

    tps1 = time.time()

    split_equal(r"/tmp/input/input.txt",r"/tmp/ngallay-20/job/splits_all/",machines_names)
    tps13 = time.time()
    dict_time['CREATE SPLITS'] =(tps13 - tps1)


    ssh("mkdir /tmp/ngallay-20/job/splits",machines_names,login)     
    ssh("mkdir /tmp/ngallay-20/job/maps",machines_names,login)    
    ssh("mkdir /tmp/ngallay-20/job/shuffles",machines_names,login)   
    ssh("mkdir /tmp/ngallay-20/job/shufflesreceived",machines_names,login)
    ssh("mkdir /tmp/ngallay-20/job/reduce",machines_names,login)
    ssh("mkdir /tmp/ngallay-20/job/output",machines_names,login)
    ssh("mkdir /tmp/ngallay-20/job/reduce_all",machines_names,login)

    tps2 = time.time()
    dict_time['CREATE FOLDERS'] =(tps2 - tps13)

    tps3 = time.time()
    
    scp_splits("/tmp/ngallay-20/job/splits_all/", "/tmp/ngallay-20/job/splits",machines_names,login,split_list)

    tps4 = time.time()
    dict_time['SENDING SPLITS'] =(tps4 - tps3)
    tps5 = time.time()

    ssh('python3 /tmp/ngallay-20/Map_slave.py 0 /tmp/ngallay-20/job/splits/S*.txt',machines_names,login)

    tps6 = time.time()
    dict_time['MAP'] = (tps6 - tps5)
    tps7 = time.time()

    ssh('python3 /tmp/ngallay-20/Map_slave.py 1 /tmp/ngallay-20/job/maps/UM*.txt',machines_names,login)

    tps8 = time.time()
    dict_time['SHUFFLE'] = (tps8 - tps7)
    tps9 = time.time()

    ssh('python3 /tmp/ngallay-20/Map_slave.py 2 /tmp/ngallay-20/job/shufflesreceived',machines_names,login)

    tps10 = time.time()
    dict_time['REDUCE'] = (tps10 - tps9)
    tps11 = time.time()

    scp_reduceFiles(r'/tmp/ngallay-20/job/reduce/*',r"/tmp/ngallay-20/job/",machines_names,login)

    tps12 = time.time()
    dict_time['RECEIVED OUTPUT'] = (tps12 - tps11)

    time_end = time.localtime(time.time())
    year, month, day, hour, minute, second, weekday, yearday, daylight = time_end

    total_time = (tps2 - tps1) + (tps4 - tps3) + (tps6 - tps5) + (tps8 - tps7) + (tps10 - tps9) + (tps12 - tps11) 
    
    return total_time,dict_time
    


result = []
dict_time  ={}
#i pour faire varier le nombre de machines
for i in range(1,2,1):
    time_job, dict_time = main(i)
    print(f'DONE #{i} ==> {time_job}')
    result.append([i,time_job,dict_time])
    clean('/tmp/ngallay-20/job')
df = pd.DataFrame(result,columns=['Nbr of machines','time','dict'])
df.to_csv(f'df_csv.csv')
