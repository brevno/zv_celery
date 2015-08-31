from celery import Celery, chord, chain

app = Celery('factorial',
             broker='amqp://:5673',
             backend='amqp://5673',
             include=['tasks'])


@app.task
def get_value_from_file(filename):
    print 'opening file', filename
    try:
        f = open(filename, mode='r')
        return int(f.read())
    except (IOError, ValueError):
        print 'Error reading value from file', filename
        return 0


@app.task
def factorial(number):
    print 'factorial recieved number', number
    return reduce(lambda a, b: a * b, range(1, number + 1), 1)


@app.task
def process_file(filename):
    return get_value_from_file.s(filename) | factorial.s()


@app.task
def xsum(numbers):
    print 'xsum of', numbers
    return sum(numbers)


def calc_factorial_sum(files):
    return chord(process_file(x) for x in files)(xsum.s())
