# Generated by Django 2.1 on 2019-06-15 19:29

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('todo_v2', '0001_initial'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='Todo',
            new_name='todo_v2',
        ),
    ]