import enum

class ChoicePriority(enum.Enum):
    H = 'H'
    M = 'M'
    L = 'L'
    C = 'C'
    
    @classmethod
    def choices(cls):
        return [(key.name,key.value) for key in cls]