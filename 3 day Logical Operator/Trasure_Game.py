
#Tressure game planning Monkey 
#

print('''
                     .-"""-.
                   _/-=-.   \\
                  (_|a a/   |_
                   / "  \   ,_)
              _    \`=' /__/
             / \_  .;--'  `-.
             \___)//      ,  \\
              \ \/;        \  \\
               \_.|         | |
                .-\ '     _/_/
              .'  _;.    (_ \\
             /  .'   `\   \\_/
            |_ /       |  |\\
           /  _)       /  / ||
          /  /       _/  /  //
          \_/       ( `-/  ||
                    /  /   \\ .-.
                    \_/     \'-'/
                             `"`
'''
)


print("""Welcome to Treasure Island.
Your mission is to find the treasure""")

print("there is two road if you choos wrong road you " 
"will fall into hole, else you are one step closer to treasure")

road = input('You\'re at a cross road. where you want to choose '
                '"Left" or Right" ?\n').lower()

if road == "left":
    print("your move to left side and move one step closer to treasure")
    lake = input('you have reach to Lake now you have to 2 option'
                 ' "wait" for boat or "swim" across the lake? \n').lower()
    if lake == "wait":
        print(" your choose the wait option after some time boat arrive "
              "and you are one step closer to treasure")
        door = input(" You have reach to the island there is 3 door"
                     ' "Red" , "Blue" , "Yellow" which door you choose ? \n').lower()
        if door == "yellow":
            print("You found the treasure !! You Win !!")
        elif door == "red":
            print("Burned by fire !! Game Over !!")
        elif door == "blue":
            print("Eaten by beasts !! Game Over !!")
        else:
            print("Invalid option you loose the game")
    
    elif lake == "swim":
        print("your choose the swim across but crocodile attack you, "
              "!! Game Over !!")
    else:
        print("Invalid option you loose the game")

elif road == "right":
    print("Fall into hole Game Over")
else:
    print("Invalid option you loose the game")