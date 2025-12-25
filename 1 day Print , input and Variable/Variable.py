
### giving error while running the code ###

#taking data and storing it in a variable and then printing it
name = input("What is your name ? : ")
# print(name)

#Question : Check the length of the user inputted name :
Contact_No = print(name + " contact number is -> " + input("what is your mobli number ? :"))   #wrong way of storing input in a variableand to print the length of it

length_of_number = len(Contact_No)
print("Length of your contact number is :"  + length_of_number)



### CHATGPT FIX ###
# Taking name
name = input("What is your name? : ")
# Taking contact number as STRING
contact_no = input("What is your mobile number? : ")
# Printing message
print(name + " contact number is -> " + contact_no)
# Finding length
length_of_number = len(contact_no)

print("Length of your contact number is :", length_of_number)



### 2 line code only ###
name = input("What is your name? : ")
print("Length of your contact number is :", len(input("Enter your mobile number: ")))
