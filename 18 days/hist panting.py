import turtle as t
import random

tim = t.Turtle()
tim.speed("fastest")
tim.penup()
tim.hideturtle()


screen = t.Screen()
step_forward = screen.textinput("Input", "Enter number of steps:")
number_of_step = int(step_forward)

t.colormode(255)
def random_color():
    r = random.randint(0,255)
    g = random.randint(0,255)
    b = random.randint(0,255)
    random_color_choose = (r,g,b)
    return random_color_choose


tim.setheading(225)
tim.forward(250)
tim.setheading(0)


for dot_count in range(1,number_of_step + 1):
    tim.dot(20,random_color())
    tim.forward(50)

    if dot_count % 10 == 0:
        tim.setheading(90)
        tim.forward(50)
        tim.setheading(180)
        tim.forward(500)
        tim.setheading(0)



screen.exitonclick()

