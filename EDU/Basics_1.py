#Команда eval (позволяет выполнить код записанный ранее как текст)
#Также может быть записан черер Ф строку
txt = '(5+5) / 2'
print(txt)
print(type(txt))
print(txt , '=' , eval(txt))
xx = 'Hello Kitty!'
txt2 = f'print("""{xx}""")'
print(txt2)
eval(txt2)

#Генератор списков
nums = [2 * k + 1 for k in range(10)]
print(nums)

#тернарный оператор (позволяет писать условие в одну строку)
num = int(input('Enter a number: '))
res = 'четное' if num % 2 == 0 else 'нечетное'
print(res)