import random


print("welcome to PyPassword Generator")
password_length = int(input("How many letter would be present in your password? : \n"))
symbol_length = int(input("How many symboll would you like ? :\n"))
number_lenght = int(input("How many number would you like ? :\n"))

letter = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']
number = ['0','1','2','3','4','5','6','7','8','9']
symbol = ['!','@','#','$','%','^','&','*']

#Easy Method for password creation like sequencing password_length = letter + symbol_length + number_lenght

# password = ""

# for char in range(1, password_length +1):
#     random_letter = random.choice(letter)
#     password = password + random_letter

#     # in one code of above 2 line
#     # password += random.choice(letter)
# for char in range(0, symbol_length):
#     password += random.choice(symbol)

# for char in range(0, number_lenght):
#     password += random.choice(number)

# print(password)




#high Method 

password_list = []

for char in range(1, password_length +1):
    random_letter = random.choice(letter)
    password_list.append(random_letter)

    # in one code of above 2 line
    # password += random.choice(letter)
for char in range(0, symbol_length):
    password_list.append(random.choice(symbol))

for char in range(0, number_lenght):
    password_list.append(random.choice(number))

#now also in list also we have sequencying also
print(password_list)

#for random sequencing we have suffle function in random 
random.shuffle(password_list)
print(password_list)

final_password =""
for each in password_list:
    final_password +=each
print(f"Your final password is : {final_password}")