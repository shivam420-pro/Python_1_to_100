import time
from turtle import Screen
from player import Player
from car_manager import CarManger
from scorboard import Scoreboard

screen = Screen()
screen.setup(width=600, height= 600)
screen.tracer(0)

player = Player()
car_manager = CarManger()
scoreboard = Scoreboard()



screen.listen()
screen.onkey(player.go_up,"Up")


game_is_on = True
while game_is_on:
    time.sleep(0.1)
    screen.update()

    car_manager.create_car()
    car_manager.move_cars()

    #detecting collision of car 
    for car in car_manager.all_car:
        if car.distance(player) < 20:
            game_is_on = False
            scoreboard.game_over()

    #When we reach to other side then we increace the more level 

    if player.is_at_finesh_line():
        player.go_to_start()
        car_manager.level_up()
        scoreboard.increase_level()


    

screen.exitonclick()