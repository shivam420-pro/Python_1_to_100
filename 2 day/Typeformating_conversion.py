#print("shivam" + 420) #Gives TypeError because we are trying to concatenate string with integer#
#correct way to do this is to convert integer to string using str() function#
print("shivam" + str(420))  #This will work fine and output will be "shivam420"#
#Another way to format strings is using f-strings (formatted string literals)#
name = "shivam"
age = 420
print(f"{name}{age}")  #This will also output "shivam420"#
#You can also use the format() method#
print("{}{}".format(name, age))  #This will also output "shivam420"#
#Type conversion in Python#
num_str = "100"
num_int = int(num_str)  #Converting string to integer#
print(num_int + 50)  #This will output 150#
num_float = float(num_str)  #Converting string to float#
print(num_float + 50.5)  #This will output 150.5#


print(type("shivam")) #<class 'str'>
print(type(420)) #<class 'int'>
print(type(10.5)) #<class 'float'>
print(type(True)) #<class 'bool'>


#for converting string into integer
print(int("100") + int("200")) #300

#for converting integer into string
int()
str()
float()
bool()


# Qestion to sholved the error 
#print("number of letter in your name : " + len(input("Enter your name: "))) resolve this problem with out error.

#solving in one line code 
print("number of letter in your name : " + str(len(input("Enter your name: "))))

#solved with variable
name = input("Enter Your Name : ")
length_of_name = len(name)
print("number of letter in your name : " + str(length_of_name))

