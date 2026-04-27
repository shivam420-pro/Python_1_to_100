import turtle as t
import random


screen = t.Screen()
step_forward = screen.textinput("Input", "Enter number of steps:")
number_step = int(step_forward)

tim = t.Turtle()
tim.pensize(10)
tim.speed(5)

#color_palette = ["red", "blue", "yellow", "pink", "purple", "orange", "goldenrod", "dark green", "magenta"]
direction =[0,90,180,270]
#number_step = int(input("Enter the number of step : "))

t.colormode(255)
def random_color():
    r = random.randint(0,255)
    g = random.randint(0,255)
    b = random.randint(0,255)
    random_color_choose = (r,g,b)
    return random_color_choose


for i in range(number_step):
    tim.color(random_color())
    # tim.color(random.choice(color_palette))
    tim.forward(30)
    tim.setheading(random.choice(direction))
    
 
