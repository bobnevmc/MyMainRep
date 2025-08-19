a = [1,2,3]
b = [1,2,3]
c = a + b
print(id(c))
c = a + [3]
print(id(c))
c+=[6]
print(id(c))
c = c + [9]
print(id(c))
print(id(a))
print(id(b))
print(a)
print(c)

a = 256
b = 256
c = 25700000000
d = 25700000000
print(a is b)
print(c is d)