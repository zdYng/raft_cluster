import asyncio
import datetime
loop = asyncio.get_event_loop()


def display_date(end_time, loop):
    print(datetime.datetime.now())
    if (loop.time() + 1.0) < end_time:
        loop.call_later(1, display_date, end_time, loop)
    else:
        loop.stop()


async def factorial(name, number):
    f = 1
    for i in range(2, number+1):
        print("Task %s: Compute factorial(%s)..." % (name, i))
        await asyncio.sleep(1)
        f *= i
    print("Task %s: factorial(%s) = %s" % (name, number, f))


if __name__ == '__main__':

    # Schedule the first call to display_date()
    # end_time = loop.time() + 5.0
    # loop.call_soon(display_date, end_time, loop)
    #
    # # Blocking call interrupted by loop.stop()
    # loop.run_forever()
    # loop.close()

    loop.run_until_complete(asyncio.gather(
        factorial("A", 2),
        factorial("B", 3),
        factorial("C", 4),
    ))
    loop.close()

