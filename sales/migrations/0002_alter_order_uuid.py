# Generated by Django 4.1.7 on 2023-03-23 11:38

from django.db import migrations, models
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ("sales", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="order",
            name="uuid",
            field=models.UUIDField(
                default=uuid.uuid4, editable=False, primary_key=True, serialize=False
            ),
        ),
    ]
