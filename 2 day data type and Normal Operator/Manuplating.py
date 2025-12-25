Age = 24

#Age now increasing by 1 "+=""
Age += 1
print(Age)

#Age now decreasing by 1 "-="
Age -= 1
print(Age)

#Age now multiplying by 2 "*="
Age *= 2
print(Age)

#Age now dividing by 2 "/="
Age /= 2
print(Age)

#Age now raised to the power of 2 "**="
Age **= 2
print(Age)

#Age now getting the remainder of 5 "%="
Age %= 5
print(Age)

#The above operators can be used with other data types as well
#For example with strings
name = "John"
name += " Doe"
print(name)




#f-strings for formatting
first_name = "Shivam"
last_name = "Don"
full_name = f"{first_name} {last_name}"
print(full_name) 


#qestion 
score = 0
height = 1.75
isWinning = True

print(f"Your score is {score}, your height is {height}, you are winning is {isWinning}")