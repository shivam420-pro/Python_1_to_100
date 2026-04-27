import turtle as t
import random


# screen = t.Screen()
# step_forward = screen.textinput("Input", "Enter number of steps:")
# number_step = int(step_forward)

tim = t.Turtle()
tim.pensize(2)
tim.speed("fastest")


t.colormode(255)
def random_color():
    r = random.randint(0,255)
    g = random.randint(0,255)
    b = random.randint(0,255)
    random_color_choose = (r,g,b)
    return random_color_choose



for i in range(200):
    tim.color(random_color())
    tim.circle(100)
    tim.setheading(tim.heading() + 10)

screen =t.Screen()
command = screen.exitonclick()