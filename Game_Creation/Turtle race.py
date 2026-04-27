import turtle as t
import random

screen = t.Screen()
screen.setup(width=500, height=400)

is_race_on = False

use_turtle = screen.textinput(
    title="Make your Bet",
    prompt="Which Turtle Color You want? - "
)

y_position = [-70, -40, -10, 20, 50, 80]

t.colormode(255)

# Convert color name → RGB
def get_rgb(color_name):
    try:
        r, g, b = screen.cv.winfo_rgb(color_name)
        return (r//256, g//256, b//256)
    except:
        return None

# Random color
def random_color():
    return (
        random.randint(0,255),
        random.randint(0,255),
        random.randint(0,255)
    )

# Build color list
colors = []

user_rgb = get_rgb(use_turtle)

if user_rgb:
    colors.append(user_rgb)
else:
    print("Invalid color! Using default red.")
    colors.append((255, 0, 0))
    user_rgb = (255, 0, 0)

# Fill remaining colors
while len(colors) < 6:
    c = random_color()
    if c not in colors:
        colors.append(c)

# Create turtles
all_turtles = []

for i in range(6):
    tim = t.Turtle(shape="turtle")
    tim.penup()
    tim.goto(x=-230, y=y_position[i])
    tim.color(colors[i])
    all_turtles.append(tim)

# Start race
if use_turtle:
    is_race_on = True

winner_color = None

while is_race_on:
    for turtle in all_turtles:
        distance = random.randint(0, 10)
        turtle.forward(distance)

        # Check finish line
        if turtle.xcor() > 230:
            is_race_on = False
            winner_color = turtle.pencolor()

# Result
print("Winner RGB:", winner_color)

if winner_color == user_rgb:
    print("🎉 You WON!")
else:
    print("😢 You LOST!")

t.done()

screen.exitonclick()