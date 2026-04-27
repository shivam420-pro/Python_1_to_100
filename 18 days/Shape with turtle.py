import turtle as t
import random as ran
tim = t.Turtle()
color_palate = ["red","blue","yellow","pink","purple","orange","goldenrod","dark green","magenta"]


def sharp_creation(number_side,t_linecolor,t_filecolor):
    angle =360/number_side
    
    tim.fillcolor(t_filecolor)
    tim.begin_fill() 
    for i in range(number_side):
        tim.color(t_linecolor)
        tim.forward(100)
        tim.right(angle)
    
    tim.end_fill()


for i in range(11,2,-1):
    color_choose = ran.choice(color_palate)
    filecolor = color_choose
    sharp_creation(i,color_choose,filecolor)
    color_palate.remove(color_choose)
    

