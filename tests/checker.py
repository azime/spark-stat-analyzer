

def same_list_tuple(list_tuple_1, list_tuple_2):
    if len(list_tuple_1) != len(list_tuple_2):
        return False
    for i, item in enumerate(list_tuple_1):
        if cmp(item, list_tuple_2[i]) != 0:
            return False
    return True