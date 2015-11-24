print("----------start init-------------------------------")
box.cfg{}
print("----------end init---------------------------------")
print()
print()

print("----------start create-------------------------------")
box.schema.space.create("ws", {engine="ws"})
print("----------end init-----------------------------------")
print()
print()

print("----------start create_index-------------------------------")
box.space.ws:create_index("i1")
print("----------end create_index---------------------------------")
print()
print()

print("----------start insert-------------------------------")
box.space.ws:insert{123456, "aa bb cc dd ee ff abc abd abe"}
print("----------end insert---------------------------------")
print()
print()


print("----------start select-------------------------------")
box.space.ws:select{"\"aa\""}
print("----------end select---------------------------------")
print()
print()
