import random

#all random functions uncase sensitive

random_randint = random.randint(1, 10)  # will return a random integer between 1 and 10 inclusive
print(random_randint)

random_random = random.random()  # will return a random float between 0.0 and 1.0
print(random_random)
print(random_random *10)  # will return a random float between 0.0 and 10.0

random_uniform = random.uniform(1, 10)  # will return a random float between 1.0 and 10.0
print(random_uniform)

random_choice = random.choice(['apple', 'banana', 'cherry'])  # will return a random element from the list
print(random_choice)


# Example usages of random module functions
