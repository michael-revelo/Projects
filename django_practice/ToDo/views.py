from django.shortcuts import render
from django.http import HttpResponseRedirect
from .models import ToDoItem

# Create your views here.

def ToDoView(request):
	all_todo_items = ToDoItem.objects.all()
	return render(request, 'ToDo.html', 
		{'all_items': all_todo_items})

def addToDo(request):
	new_item = ToDoItem(content = request.POST['content'])
	new_item.save()
	return HttpResponseRedirect('/ToDo/')

def deleteToDo(request, todo_id):
	item_to_delete = ToDoItem.objects.get(id = todo_id)
	item_to_delete.delete()
	return HttpResponseRedirect('/ToDo/')
