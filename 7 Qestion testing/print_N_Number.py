# Write a program to print numbers from 1 to N, where N is given by the user.

print("print the number till user want in list and as well as in string")

user_input  = int(input("Enter the number till you want to print Number :- \n"))

list_number = []
print(f"number from 1 to {user_input} is :")

for number_list in range(1,user_input+1):
    list_number.append(number_list)
    
print(list_number)

for number in range(1,user_input+1):
    print(number)

