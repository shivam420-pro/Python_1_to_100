def prime(num):
   if num <2 :
        return False
   else:
       for i in range(2,num):
           print(i)
           if num % i == 0:                
            return True

            
print(prime(10))
 
