from turtle import Screen
import time
from snake_import import Snake
from snake_food import Food
from snake_score import Scoreboard
from tkinter import messagebox


screen = Screen()
screen.setup(width=600,height=600)
screen.bgcolor("black")
screen.title("Snake Game")
screen.tracer(0)

snake = Snake()
food = Food()
scoreboard = Scoreboard()


screen.listen()
screen.onkey(snake.up,"Up")
screen.onkey(snake.down,"Down")
screen.onkey(snake.left,"Left")
screen.onkey(snake.right,"Right")

game_is_on = True
while game_is_on:
    screen.update()
    time.sleep(0.12)
    snake.move()


    # Collition of food and snake
    if snake.head.distance(food) < 15 :
        food.refresh()
        snake.extend()
        scoreboard.increase_score()


    # Detect collision with tail

    for segment in snake.segments:
        if segment == snake.head:
            pass
        elif snake.head.distance(segment) <10:
            game_is_on = False
            scoreboard.game_over()

    # if head collides with any segment in the tail
    # trigger the Game over ..!


    #Detect collision with wall
    if snake.head.xcor() > 280 or snake.head.xcor() < -280 or snake.head.ycor() > 280 or snake.head.ycor() < -280:
        game_is_on = False
        scoreboard.game_over()

    if not game_is_on:
        scoreboard.game_over()

        answer = messagebox.askyesno(
            "Game Over",
            "Do you want to play again?"
        )

        if answer:   # YES → restart
            snake.reset()
            scoreboard.reset()
            food.refresh()
            game_is_on = True
        else:        # NO → exit
            screen.bye()


screen.exitonclick()
