
import random

for i in range(1000000):
  print( str(random.random())+","+str(int(random.expovariate(1/5))) )

