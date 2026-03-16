def is_leap_year(year):
    # Write your code here. 
    # Don't change the function name.
    if (year % 4 ==0 and year % 100 !=0 ) or (year % 400 == 0):
        return True
    else:
        return False
    
leap_year_check = is_leap_year(2400)
