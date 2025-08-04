from dns.entropy import between

A = [1,2,3 , 4 ,5]

B = [6,7]

#сложение
C = A + B
C += [6]  #самый быстрый способ что то добавить
C = [100] + C

#удаление элемента
C[1:2] = []

#замена элементов
C[4:6] = [33,22]
print(C)

#Вставка элементов
C[6:6] = [777,8888,9999]
print(C)

#Присваивание значения
C[2:3] = ['Вставка']
print(C)

#формирование списков через цицла и условия
S = [5 if x > 10 else x for x in range(20) if not x % 2 == 1]
print(S)

#создание пустого списка
res = [['' for i in range(5)] for j in range(5)]
print(res)

#Удаление из списка
lst = [1, 2, 3, 2, 4, 5]
for val in [2, 4]:
    while val in lst:
        lst.remove(val)
# Результат: [1, 3, 5]