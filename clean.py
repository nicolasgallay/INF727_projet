import subprocess


login="ngallay-20"
file = open('adresse_ip.txt','r')
machines_names = file.read().splitlines()   
    
def ssh(command):
    listproc = []
    timer=100

    for name in machines_names:
        proc = subprocess.Popen(["ssh",login+"@"+name,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)
        print(name)
        
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


def clean(file_path):
    listproc = []
    timer=100
    for name in machines_names:
        proc = subprocess.Popen(['ssh',  login + '@' + name, 'rm -rf ', file_path],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
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

clean('/tmp/ngallay-20/')