from turtle import Turtle

class Scoreboard(Turtle):
    
    def __init__(self):
        super().__init__()
        self.score = 0
        with open("data.txt") as data:
            content = data.read()
            if content:
                self.high_score = int(content)
            else:
                self.high_score = 0
        self.penup()
        self.color("white")
        self.goto(0,260)
        self.hideturtle()
        self.update_scoreboard()


    def update_scoreboard(self):
        self.clear()
        self.write(f"Score : {self.score} High Score :{self.high_score}",align="center",font=("Arial",24,"normal"))
        

    # def game_over(self):
    #     self.goto(0,0)
    #     self.write(f"Game Over ..! :",align="center",font=("Arial",24,"normal"))

    def reset(self):
        if self.score > self.high_score:
            self.high_score = self.score
            with open("data.txt",mode = "w") as data:
                data.write(f"{self.high_score}")
        self.score = 0
        self.update_scoreboard()



    def increase_score(self):
        self.score +=1
        self.clear()
        self.update_scoreboard()
