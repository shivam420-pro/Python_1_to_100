def is_leap_year(year):
    # Write your code here. 
    # Don't change the function name.
    if (year % 4 ==0 and year % 100 !=0 ) or (year % 400 == 0):
        return True
    else:
        return False
    

input_year = int(input("Enter a year to check if it's a leap year: "))

leap_year_check = is_leap_year(input_year)
if leap_year_check:
    print(f"{input_year} is a leap year.")
else:
    print(f"{input_year} is not a leap year.")
# To check if the function is working correctly
# print(is_leap_year(2000)) # True  