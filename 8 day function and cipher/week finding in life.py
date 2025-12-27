# The function life_in_weeks() takes your current age as input.

# It assumes the maximum lifespan is 90 years.

# It calculates how many years you have left.

# Then it converts those years into weeks.

# Finally, it prints a proper English sentence using an f-string, ending with a full stop, exactly as required.


def life_week_checking(age,max_age=90):
    pending_age =max_age - age
    pending_week = pending_age*52
    print(f" pending week till 90 year is {pending_week}")



user_age = int(input('''Enter your age to identify 
                     till 90 year how may week is pending :- '''))

life_week_checking(user_age)