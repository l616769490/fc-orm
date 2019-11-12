def aaa():
    try:
        a = 1 / 0
    except Exception as e:
        print('==============')
        raise Exception('aaa')


if __name__ == "__main__":
    print(aaa())

