score = [134,134,194,189,134,168,157,108,157,187,135,179,1461,102]

#sum function working
print(sum(score))


#need how sum function working with list 
sum = 0

for score_sum in score:
    sum += score_sum
print(sum)


#max function working
print(max(score))

max_checking = 0

for max_work in score:
    if max_work > max_checking:
        max_checking = max_work
    else:   #this can be a opestinoal 
        max_checking
print(max_checking)