def chunk_list(data:list, chunk_size: int):
    return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]