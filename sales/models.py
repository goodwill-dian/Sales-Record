from django.db import models
import uuid
from sales_records.enums import choice_channel, choice_priority

class BaseModel(models.Model):
    uuid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    class Meta:
        abstract = True

class Region(models.Model):
    name = models.CharField(max_length=100, unique=True)
    
class Country(models.Model):
    name = models.CharField(max_length=100, unique=True)
    
class ItemType(models.Model):
    type = models.CharField(max_length=50, unique=True)

class Order(BaseModel):
    region = models.ForeignKey(Region, on_delete=models.CASCADE)
    country = models.ForeignKey(Country, on_delete=models.CASCADE)
    item_type = models.ForeignKey(ItemType, on_delete=models.CASCADE)

    sales_channel = models.CharField(max_length=10, choices=choice_channel.ChoiceChannel.choices())
    order_priority = models.CharField(max_length=10, choices=choice_priority.ChoicePriority.choices())
 
    order_date = models.DateField()
    order_id = models.IntegerField()
    ship_date = models.DateField()
    
    units_sold = models.IntegerField()
    unit_price = models.FloatField()
    unit_cost = models.FloatField()
    
    total_revenue = models.FloatField()
    total_cost = models.FloatField()
    total_profit = models.FloatField()