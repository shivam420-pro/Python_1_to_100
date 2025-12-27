#Input a number and print its reverse.
#Example:  123 → 321

num = int(input("Enter the number to reverse the digit :- "))

reverse = 0

while num > 0:
    digit = num % 10   #take the last digit from the digit 
    reverse = reverse * 10 + digit  
    num = num // 10

print("Reversed number is:", reverse)