import os 
class Config():
    
    def __init__(self,env_variables:list[str]):
        for env in env_variables:
            env_value = os.getenv(env) 
            if env_value != None : setattr(self,env,env_value) 
            else : raise ValueError("Environment variable not set : {0} ".format(env))