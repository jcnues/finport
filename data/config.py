import os

class Config:
    @property
    def mongo_user(self):
        return os.getenv("MONGO_USER")
    
    @property
    def mongo_pass(self):
        return os.getenv("MONGO_PASS")