from turtle import Screen, Turtle
from paddle import Paddle
from ball import Ball
import time
from scoreboard import Scoreboard

screen = Screen()
screen.bgcolor("Black")
screen.setup(width=800,height=600)
screen.title("Pong Game")
screen.tracer(0)

r_paddle = Paddle((350,0))
l_paddle = Paddle((-350,0))

ball = Ball()
scoreboards = Scoreboard()


screen.listen()
# right side paddle movement 
screen.onkey(r_paddle.go_up,"Up")
screen.onkey(r_paddle.go_down,"Down")

#left side paddle movement
screen.onkey(l_paddle.go_up,"w")
screen.onkey(l_paddle.go_down,"s")

game_is_on = True
while game_is_on:
    time.sleep(ball.move_speed)
    screen.update()
    ball.move()

    #Detecting the collition of ball with wall
    if ball.ycor() > 280 or ball.ycor() < -280 :
        #need to bounce 
        ball.bounce_y()

    # Detecting the collition of ball with paddle
    if ball.distance(r_paddle) < 50 and ball.xcor() > 320 or ball.distance(l_paddle) < 50 and ball.xcor() > -320:
        ball.bounce_x()

    if ball.xcor() > 380:
        ball.reset_position()
        scoreboards.l_point()

    if ball.xcor() < -380:
        ball.reset_position()
        scoreboards.r_point()
    


    
screen.exitonclick()