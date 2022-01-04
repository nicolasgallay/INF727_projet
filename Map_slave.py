import sys
import os
import re
import hashlib
import socket
import subprocess as subprocess #lance des processus lourd
from os import listdir
from os.path import isfile, join
from collections import Counter

LOGIN="ngallay-20"

def list_of_machines():
    file = open(r'/tmp/ngallay-20/job/machine_names.txt','r',encoding='ISO-8859-1')
    machines_liste = file.read().split('\n')
    del machines_liste[-1]
    return machines_liste

LIST_MACHINES = list_of_machines()

def get_hostname():
    return socket.gethostname()
    
HOSTNAME = get_hostname()


def scp(localPath,distantPath,machine_name):
    listproc = []
    timer=5
    # for  names in machines_names:
    proc = subprocess.Popen(["scp",localPath,LOGIN+"@"+machine_name+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
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

def map_function(split_path):

        split_file = open(split_path,'r',encoding='ISO-8859-1')
        words = split_file.read().split()
        #  words = split_file.read().split(' ')

        #on récupère le numéro du split
        file_number = re.findall(r"(\d+)\.txt",(os.path.basename(split_file.name).split())[0])[0]
        word_list = []

        with open('/tmp/ngallay-20/job/maps/UM'+file_number+'.txt', 'w',encoding='ISO-8859-1') as file_map:
            
            for word in words:
                # if word != '':
                word_1 = str(word.strip('\n') + ' 1')
                word_list.append(word_1)

            file_map.write("\n".join(word_list))
        
        split_file.close()
        print('End of map for S' + file_number +'.txt')
        return

                
def shuffle_function(map_path):

        map_file = open(map_path,'r',encoding='ISO-8859-1')
        lignes = map_file.read().split('\n')
        dict_shuffle= {}
        file_number = re.findall(r"(\d+)\.txt",(os.path.basename(map_file.name).split())[0])[0]
        
    
        #calcul du hash pour chaque mot
        for index, ligne in enumerate(lignes):
            hashcode = (str(int.from_bytes(hashlib.sha256(ligne.encode("ISO-8859-1")).digest()[:4], 'little')))
            dict_shuffle.setdefault(hashcode,[]).append(ligne)
        for key, value in dict_shuffle.items():
            
            with open('/tmp/ngallay-20/job/shuffles/'+LIST_MACHINES[int(key) % len(list_of_machines())]+'_from_'+HOSTNAME+'.txt', 'a',encoding='ISO-8859-1') as file_shuffle:
                file_shuffle.write("\n".join(value))
                file_shuffle.write("\n".join('\n'))
                file_shuffle.close()

        shuffles_files = [f for f in listdir('/tmp/ngallay-20/job/shuffles/')] #if isfile(join('/tmp/ngallay-20/job/shuffles/', f))]
        for files in shuffles_files:
            # receiver_number = int(key) % len(LIST_MACHINES)
            name = re.findall('^[^_]*', files)[0]

            scp('/tmp/ngallay-20/job/shuffles/'+ files, '/tmp/ngallay-20/job/shufflesreceived', name)  

        # print('End of shuffles for UM' + file_number +'.txt')

        return

def reduce_function(reduce_path):

    files = [f for f in listdir(reduce_path) if isfile(join(reduce_path, f))]
    c = Counter()
    for file in files:
        
        with open('/tmp/ngallay-20/job/shufflesreceived/'+file,"r",encoding='ISO-8859-1') as file_map:
            word = file_map.read().split("\n")
            c.update(word_count for word_count in word)
            file_map.close() 
        
    with open('/tmp/ngallay-20/job/reduce/reduce-'+HOSTNAME+'.txt', 'w',encoding='ISO-8859-1') as file_reduce:
        for key, value in c.items():
             file_reduce.write("".join('%s %s\n' % (key.split(" ")[0], value)))
        file_reduce.close()
    return


def main():
  if len(sys.argv) != 3:
    print ('MISSING ARGUMENT, usage: ./map_slave.py -type de job  -path (D:\Project\..)')
    sys.exit(1)
    
  func_arg = sys.argv[1]
  path = sys.argv[2]
    
  if func_arg == '0':
    map_function(path)
    
  elif func_arg == '1':
    shuffle_function(path)
    
  elif func_arg == '2':
    reduce_function(path)
    
  else:
    print ('job doesn\'t exist')
    sys.exit(1)
    
    
if __name__ == '__main__':
  main()