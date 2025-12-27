def calculate_love_score(name1, name2):
    combine_name = (name1+name2).lower()
    
    
    t= combine_name.count("t")
    r= combine_name.count("r")
    u= combine_name.count("u")
    e= combine_name.count("e")
    total_ture_score =t+r+u+e
    
    l = combine_name.count("l")
    o = combine_name.count("o")
    v = combine_name.count("v")
    e = combine_name.count("e")
    love_score = l + o + v + e
    
    total_score = int(str(total_ture_score)+str(love_score))
    print(total_score)
    
calculate_love_score("shivam", "angira")