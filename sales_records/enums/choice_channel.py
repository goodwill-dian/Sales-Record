import enum

class ChoiceChannel(enum.Enum):
    ONLINE = 'ONLINE'
    OFFLINE = 'OFFLINE'
    
    @classmethod
    def choices(cls):
        return [(key.name,key.value) for key in cls]