import sys
from collections import Counter
from collections import OrderedDict
import time
import json

#Premier comptage en séquentiel pur
def counter(filename):
    
    start_op1 = time.time_ns()
    
    with open(filename, encoding="ISO-8859-1") as f:
        words = f.read().split()
    
    c = Counter()
    c.update(word for word in words)
    
    elapsed_op1 = (time.time_ns() - start_op1)*10**(-9)
    start_op2 = time.time_ns()
    
    #tri alphabétique des clés  
    c2 = OrderedDict( sorted(c.items(), key=lambda t: t[0]))
    #tri par valeur 
    c3 = OrderedDict(sorted(c2.items(), key=lambda t: t[1], reverse = True))
    
    elapsed_op2 = (time.time_ns() - start_op2)*10**(-9)
    
    # # for keys, values in c3.items():
    # #     print(keys, values)
    # with open(r'/Users/nicolasgallay/OneDrive/Telecom-Paris/Python-Telecom-Paris/INF727/INF727_git/output_2/output_ not_distibuted.txt','w') as output_file:
    #   output_file.write(json.dumps(c3))
    
    with open(r'/Users/nicolasgallay/OneDrive/Telecom-Paris/Python-Telecom-Paris/INF727/INF727_git/output_2/output_ not_distibuted.txt','w',encoding='ISO-8859-1') as outfile_sorted:
      for key, value in c3.items(): 
          outfile_sorted.write('%s : %s\n' % (key, value))
    
    return  elapsed_op1, elapsed_op2
    

def main():
  
  start = time.time()
  
  if len(sys.argv) != 2:
    print ('usage: run count_word.py file')
    sys.exit(1)

  filename = sys.argv[1]
  elapsed_op1, elapsed_op2 = counter(filename)
  
  elapsed = time.time() - start
  
  print(f'\nTemps operation de comptage des occurences : {elapsed_op1:.4}s')
  print(f'Temps opration de tri : {elapsed_op2:.4}s')
  print(f'Temps total d\'exécution : {elapsed:.4}s')
  
  
if __name__ == '__main__':
  main()
