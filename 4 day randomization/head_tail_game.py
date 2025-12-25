#head and Tail game with randomization

import random
import os
os.system('cls' if os.name == 'nt' else 'clear')

print("Welcome to Head and Tail Game")

random_head_tail = random.randint(0,1)  # 0 for Head and 1 for Tail
if random_head_tail == 0:
    print("Head")
else:
    print("Tail")
