from django.shortcuts import render, redirect
from django.views.decorators.http import require_POST
from . models import todo_v2
from . forms import TodoForm

def index(request):
    todo_list = todo_v2.objects.order_by('id')
    form = TodoForm()
    context = {'todo_list' : todo_list, 'form' : form}
    return render(request, 'index.html', context)

@require_POST
def addTodo(request):
    form = TodoForm(request.POST)
    if form.is_valid():
        new_todo = todo_v2(text=request.POST['text'])
        new_todo.save()
    return redirect('index')

def completeTodo(request, todo_id):
    todo = todo_v2.objects.get(pk=todo_id)
    todo.complete = True
    todo.save()
    return redirect('index')

def deleteCompleted(request):
    todo_v2.objects.filter(complete__exact=True).delete()
    return redirect('index')

def deleteAll(request):
    todo_v2.objects.all().delete()
    return redirect('index')