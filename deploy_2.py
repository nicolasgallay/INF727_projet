import subprocess


login="ngallay-20"
with open('adresse_ip.txt','r',encoding='utf-8') as file:
    machines_names = file.read().splitlines()   
    
def ssh(command):
    listproc = []
    timer=100

    for names in machines_names:
    #        machine = "tp-4b01-0"+str(i)
        proc = subprocess.Popen(["ssh",login+"@"+names,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)
        
    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code :
                print(machines_names[i]+" out: '{}'".format(out))
                print(machines_names[i]+" err: '{}'".format(err))
                print(machines_names[i]+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(machines_names[i]+" timeout")


def scp(localPath,distantPath):
    listproc = []
    timer=100
    for names in machines_names:
        proc = subprocess.Popen(["scp",localPath,login+"@"+names+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(machines_names[i]+" out: '{}'".format(out))
                print(machines_names[i]+" err: '{}'".format(err))
                print(machines_names[i]+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(machines_names[i]+" timeout")    

def scp2(localPath,distantPath,machines_names,login):
    listproc = []

    for  names in machines_names:
        proc = subprocess.Popen(["scp",localPath,login+"@"+names+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(machines_names[i]+" out: '{}'".format(out))
                print(machines_names[i]+" err: '{}'".format(err))
                print(machines_names[i]+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(machines_names[i]+" timeout") 

machine_worker = ['tp-4b01-17']
ssh("mkdir /tmp/ngallay-20")
ssh("mkdir /tmp/ngallay-20/job")
ssh("mkdir /tmp/ngallay-20/job/splits_all")
scp("adresse_ip.txt", "/tmp/ngallay-20/machines.txt")
scp("./Map_slave.py", "/tmp/ngallay-20")
scp2("./Master.py", "/tmp/ngallay-20",machine_worker,'ngallay-20')
