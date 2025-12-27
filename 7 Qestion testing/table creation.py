# Print the multiplication table of a number

Table_need = int(input("Enter which tabel you want to print :- "))
up_tonumber = int(input("Enter till what multipla need :-"))


for table in range(1,up_tonumber+1):
    print(Table_need,"x", table ,"x",table*Table_need)
