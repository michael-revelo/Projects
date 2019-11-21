from django import forms 

class TodoForm(forms.Form):
    text = forms.CharField(max_length=40, 
        widget=forms.TextInput(
            attrs={'class' : 'form-control', 
            	   'placeholder' : 'Enter your to-do e.g. Keep practicing django', 
            	   'aria-label' : 'Todo', 
            	   'aria-describedby' : 'add-btn'}))
